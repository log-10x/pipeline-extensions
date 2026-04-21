package com.log10x.ext.cloud.index.query.object;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.pipeline.launch.PipelineLaunchOptions;
import com.log10x.api.util.MapperUtil;
import static com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.MDC_QUERY_ID;

import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.QueryLogLevel;
import com.log10x.ext.cloud.index.shared.BaseIndexReader;
import com.log10x.ext.cloud.index.util.stream.ByteRangeFilterInputStream;

/**
 * This class reads a KV object byte range from 
 * a target container (e.g. AWS S3 bucket). Lines read from input are
 * optionally filtered against a set of search terms.
 * 
 * To learn more see {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/object/stream.yaml}
 */
public class IndexObjectQueryReader extends BaseIndexReader {

	private static final Logger logger = LogManager.getLogger(IndexObjectQueryReader.class);

	private final Reader reader;

	private final String queryId;
	private final String workerID;
	private final List<String> logLevels;

	private boolean shouldLog(QueryLogLevel level) {
		if (logLevels == null || logLevels.isEmpty()) {
			return level != QueryLogLevel.DEBUG;
		}
		return logLevels.contains(level.name());
	}

	public IndexObjectQueryReader(IndexQueryObjectOptions options,
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {

		super(options, indexAccessor, evaluatorBean);

		this.queryId = options.queryObjectID;
		this.workerID = (String) evaluatorBean.env(PipelineLaunchOptions.UNIQUE_ID);
		this.logLevels = options.queryObjectLogLevels;

		ThreadContext.put(MDC_QUERY_ID, this.queryId);

		InputStream	inputStream = this.createInputStream(options);

		this.reader = this.createTermReader(inputStream);
	}
	
	// How long past the query's overall deadline we still let a stream worker
	// start. Anything past this is treated as an abandoned query and skipped.
	// Set generously because the original query deadline is shared across the
	// coordinator's scan phase + SQS dispatch + stream-worker execution, and
	// worker dispatch latency on a busy cluster can easily eat several
	// minutes before this worker picks up its message.
	private static final long POST_DEADLINE_GRACE_MS = 10 * 60 * 1000L;

	private InputStream createInputStream(IndexQueryObjectOptions options) throws IOException {

		long now = System.currentTimeMillis();

		long elapseTime = options.elapseTime();

		// Only skip if we are far past the query deadline — e.g. a stale
		// message that lingered in SQS after the user already gave up. Short
		// overshoots are normal and must NOT prevent the worker from running,
		// otherwise the scan phase eating the default budget starves every
		// worker and the user sees zero events.
		if ((elapseTime != 0) &&
			(now > elapseTime + POST_DEADLINE_GRACE_MS)) {

			logger.info("skipping query {}: stale (past deadline + grace)", options.ID());

			if (shouldLog(QueryLogLevel.ERROR)) {
				this.indexAccessor.logQueryEvent(this.queryId, this.workerID,
						QueryLogLevel.ERROR,
						String.format("stream worker skipped: stale message %dms past deadline (grace=%dms)",
							now - elapseTime, POST_DEADLINE_GRACE_MS),
						null);
			}

			return InputStream.nullInputStream();
		}

		int size = options.size();

		if (size == 0) {
			if (shouldLog(QueryLogLevel.ERROR)) {
				this.indexAccessor.logQueryEvent(this.queryId, this.workerID,
						QueryLogLevel.ERROR,
						"stream worker error: empty byteRanges for object=" + options.queryObjectTargetObject,
						null);
			}
			throw new IllegalArgumentException("byteRanges");
		}

		TreeMap<Long, Integer> byteRanges = new TreeMap<>();

		long initialOffset = Long.MAX_VALUE;

		for (int i = 0; i < size; i++) {
			initialOffset = Math.min(initialOffset, options.offset(i));
		}

		for (int i = 0; i < size; i++) {

			long off = options.offset(i);
			int len = options.length(i);

			byteRanges.put(off - initialOffset, len);
		}

		long rangeEnd = byteRanges.lastKey() + byteRanges.lastEntry().getValue();

		Map<String, Object> perfData = new LinkedHashMap<>();
		perfData.put("object", options.queryObjectTargetObject);
		perfData.put("byteRanges", size);
		perfData.put("fetchBytes", rangeEnd);

		if (shouldLog(QueryLogLevel.PERF)) {
			this.indexAccessor.logQueryEvent(this.queryId, this.workerID,
					QueryLogLevel.PERF,
					String.format("stream worker started: object=%s, byteRanges=%d, fetchRange=[%d,%d] (%d bytes)",
							options.queryObjectTargetObject, size, initialOffset, initialOffset + rangeEnd, rangeEnd),
					perfData);
		}

		InputStream storageStream;
		long fetchStartMs = System.currentTimeMillis();
		try {
			storageStream = this.indexAccessor.readObject(
				options.queryObjectTargetObject, initialOffset, (int)rangeEnd);
		} catch (Exception e) {
			logger.error("error reading object: " + options.queryObjectTargetObject, e);
			if (shouldLog(QueryLogLevel.ERROR)) {
				this.indexAccessor.logQueryEvent(this.queryId, this.workerID,
						QueryLogLevel.ERROR,
						String.format("stream worker error: failed reading object=%s, offset=%d, bytes=%d: %s",
							options.queryObjectTargetObject, initialOffset, (int)rangeEnd, e.getMessage()),
						Map.of("object", options.queryObjectTargetObject,
							"offset", initialOffset,
							"requestedBytes", (int)rangeEnd,
							"error", e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage())));
			}
			throw e;
		}

		// #3 Per-fetch S3 read. The prior log only recorded the REQUESTED
		// byte range; it said nothing about the open call's latency, the
		// InputStream's origin, or whether the accessor actually produced
		// a usable stream. For the "stream worker complete: fetched 0 bytes"
		// symptom, this is the only event that can distinguish "S3 returned
		// empty" from "parser never ran" from "filter rejected everything".
		if (shouldLog(QueryLogLevel.DEBUG)) {
			this.indexAccessor.logQueryEvent(this.queryId, this.workerID,
				QueryLogLevel.DEBUG,
				String.format("stream worker fetch: object=%s, offset=%d, requestedBytes=%d, openMs=%d, streamType=%s",
					options.queryObjectTargetObject, initialOffset, (int)rangeEnd,
					System.currentTimeMillis() - fetchStartMs,
					storageStream == null ? "null" : storageStream.getClass().getSimpleName()),
				Map.of("object", options.queryObjectTargetObject,
					"offset", initialOffset,
					"requestedBytes", (int)rangeEnd,
					"openMs", System.currentTimeMillis() - fetchStartMs,
					"streamType", storageStream == null ? "null" : storageStream.getClass().getSimpleName()));
		}

		return new ByteRangeFilterInputStream(storageStream,
			byteRanges);

	}
	
	protected Reader createTermReader(InputStream inputStream) {
		
		return new InputStreamReader(inputStream);
		
//		IndexQueryObjectOptions options = (IndexQueryObjectOptions)this.options;
//		
//		switch (options.queryObjectTerms.size()) {
//		
//			case 0: return new InputStreamReader(inputStream);
//			
//			case 1: return new SingleTermFilterReader(inputStream,
//						options.queryObjectTerms.get(0));
//				
//			default: return new MultiTermFilterReader(inputStream, 
//					 	options.queryObjectTerms); 
//		}	
	}
	
	public IndexObjectQueryReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws IllegalArgumentException, IOException{
			
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexQueryObjectOptions.class), null, evaluatorBean);
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		return reader.read(cbuf, off, len);
	}

	@Override
	public void close() throws IOException {
		
		reader.close();	
		super.close();
	}
}
