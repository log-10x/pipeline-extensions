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
	
	private InputStream createInputStream(IndexQueryObjectOptions options) throws IOException {
		
		long now = System.currentTimeMillis();
		
		long elapseTime = options.elapseTime();
		
		if ((elapseTime != 0) &&
			(now > elapseTime)) {

			logger.info("skipping query {}: elapsed time exceeded", options.ID());

			if (shouldLog(QueryLogLevel.ERROR)) {
				this.indexAccessor.logQueryEvent(this.queryId, this.workerID,
						QueryLogLevel.ERROR,
						String.format("stream worker skipped: processing time limit exceeded (elapsed %dms over deadline)",
							now - elapseTime),
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
						null);
			}
			throw e;
		}

		if (shouldLog(QueryLogLevel.DEBUG)) {
			this.indexAccessor.logQueryEvent(this.queryId, this.workerID,
				QueryLogLevel.DEBUG,
				String.format("stream worker read: object=%s, offset=%d, bytes=%d",
					options.queryObjectTargetObject, initialOffset, (int)rangeEnd),
				null);
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
