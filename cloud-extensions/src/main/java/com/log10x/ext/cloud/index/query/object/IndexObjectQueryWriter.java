package com.log10x.ext.cloud.index.query.object;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.pipeline.launch.PipelineLaunchOptions;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.QueryLogLevel;
import com.log10x.ext.cloud.index.shared.BaseIndexWriter;

/**
 * This Writer updates the volume of events fetched from a KV object byte range from 
 * a target container (e.g. AWS S3 bucket). 
 * 
 * To learn more see {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/object/stream.yaml}
 */
public class IndexObjectQueryWriter extends BaseIndexWriter {

	private final AtomicLong utf8Sizes;

	private final String objectByteRangesKey;

	private final String queryId;
	private final String workerID;
	private final List<String> logLevels;

	private boolean shouldLog(QueryLogLevel level) {
		if (logLevels == null || logLevels.isEmpty()) {
			return level != QueryLogLevel.DEBUG;
		}
		return logLevels.contains(level.name());
	}
	
	/**
	 * this constructor is invoked by the 10x engine.
	 * 
	 * @param 	args
	 * 			the values of the 'IndexWrite' option group
	 * 		    instance for which the IndexFilterWriter is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this input
	 * 			to interact with the 10x run-time 
	 * @throws IllegalArgumentException 
	 * @throws NoSuchAlgorithmException 
	 */
	public IndexObjectQueryWriter(Map<String, Object> args, EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException, IllegalArgumentException{
		
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexQueryObjectOptions.class), null, evaluatorBean);
	}
	
	protected IndexObjectQueryWriter(IndexQueryObjectOptions options, 
		ObjectStorageIndexAccessor indexAccessor,
		EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException {
		
		super(options, indexAccessor, evaluatorBean);

		this.queryId = options.queryObjectID;
		this.workerID = (String) evaluatorBean.env(PipelineLaunchOptions.UNIQUE_ID);
		this.logLevels = options.queryObjectLogLevels;

		this.utf8Sizes = new AtomicLong();
		
		MessageDigest md = MessageDigest.getInstance("MD5");

		md.update(options.queryObjectTarget.getBytes());
		md.update(options.queryObjectTargetObject.getBytes());
		
	    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		
		for (long rangeValue : options.queryObjectByteRanges) {
			
			buffer.putLong(0, rangeValue);
			md.update(buffer);			
		}
		
		this.objectByteRangesKey = Base64.getEncoder().encodeToString(md.digest());
	}

	@Override
	public synchronized void flush() throws IOException {

		try {

			utf8Sizes.addAndGet(Long.parseLong(currChars.builder.toString()));

		} catch (Exception e) {

			if (shouldLog(QueryLogLevel.ERROR)) {
				indexAccessor.logQueryEvent(this.queryId, this.workerID,
						QueryLogLevel.ERROR,
						String.format("stream worker error: could not parse utf8Size: %s", this.currChars),
						null);
			}
			throw new IllegalStateException("could not parse utf8Size: " + this.currChars, e);

		} finally {
			super.flush();
		}
	}

	@Override
	public synchronized void close() throws IOException {

		try {

			String basePath = indexAccessor.indexObjectPath(IndexObjectType.query, this.options.target());

			String key = (new StringBuilder(basePath))
					.append(indexAccessor.keyPathSeperator())
					.append(((IndexQueryObjectOptions)this.options).queryObjectID)
					.append(indexAccessor.keyPathSeperator())
					.append(this.objectByteRangesKey)
					.toString();

			indexAccessor.putObject(key, utf8Sizes.toString(), null, 0L, null);

			long totalBytes = utf8Sizes.get();
			
			if (shouldLog(QueryLogLevel.PERF)) {
				indexAccessor.logQueryEvent(this.queryId, this.workerID,
						QueryLogLevel.PERF,
						String.format("stream worker complete: fetched %d bytes", totalBytes),
						java.util.Map.of("fetchedBytes", totalBytes));
			}

		} catch (Exception e) {

			if (shouldLog(QueryLogLevel.ERROR)) {
				indexAccessor.logQueryEvent(this.queryId, this.workerID,
						QueryLogLevel.ERROR,
						String.format("stream worker error: failed during close: %s", e.getMessage()),
						null);
			}
			throw e;
		}

		super.close();
	}
}
