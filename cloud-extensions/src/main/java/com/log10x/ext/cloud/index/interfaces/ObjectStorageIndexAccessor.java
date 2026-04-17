package com.log10x.ext.cloud.index.interfaces;

import java.util.Map;

import com.log10x.api.pipeline.endpoint.PipelineEndpointAccessor;
import com.log10x.ext.cloud.index.access.AWSIndexAccess;

/**
 * This interface provides a simple abstraction over a the different services
 * needed for '10x Object Storage Analyzer'.
 * 
 * Those services include access to KV value storage and additional invocations
 * of the '10x pipeline'.
 * 
 * For storage access see {@link ObjectStorageAccessor}
 * For pipeline invocations see {@link PipelineEndpointAccessor}
 * 
 * For an AWS implementation, see: {@link AWSIndexAccess}
 */
public interface ObjectStorageIndexAccessor extends PipelineEndpointAccessor, ObjectStorageAccessor {

	public static final String MDC_QUERY_ID = "queryId";

	public static enum QueryLogLevel {
		ERROR, INFO, DEBUG, PERF
	}

	public static enum IndexObjectType {

		template("t"),
		byteRange("b"),
		reverseIndex("r"),
		query("q"),
		queryResults("qr");
		
		public final String key;
		
		private IndexObjectType(String key) {
			this.key = key;
		}
	}
	
	// Index object tags
	
	/*
	 * The string values added to the a bloom filter index object's tags. 
	 * For debug purposes only.
	 */
	public static final String DEBUG_VALUES_TAG = "values";
	
	// 10x data (templates/reverse index) key path prefixes

	/**
	 * the top level prefix under which tenx index objects are stored 
	 */
	public static final String TENX_PREFIX = "tenx";
	
	/**
	 * Returns the path from which read/write a target index object type
	 * 
	 * @param 	objectType 
	 * 			The type of index object to read/write
	 * 
	 * @param 	prefix 
	 * 			App prefix
	 * 
	 * @param 	topPrefix 
	 * 			Specifies whether to prepend the TENX_PREFIX to the result
	 * 
	 * @return	the path within the underlying object storage under which to read/write the index object
	 */
	public default String indexObjectPath(IndexObjectType objectType, 
		String prefix) {
		
		String key = objectType.key;
		
		return (key != null) ?
			String.join(this.keyPathSeperator(), TENX_PREFIX, prefix, key) :
			prefix;
	}
	
	/**
	 * Translates a value to a key that can be used within the underlying storage.
	 * 
	 * @param 	key	
	 * 			value to translate
	 
	 * @return	translated key
	 */
	public default String expandIndexObjectKey(String key) {
		return key;
	}

	/**
	 * Logs a structured query lifecycle event for user-visible tracking via CloudWatch Logs.
	 *
	 * @param 	queryID   the query UUID
	 * @param 	workerID  the pipeline UUID of the worker
	 * @param 	level     the log level (ERROR, INFO, DEBUG, PERF)
	 * @param 	message   the log message
	 * @param 	metadata  optional key-value pairs (e.g., bytes streamed, elapsed time)
	 */
	public default void logQueryEvent(String queryID, String workerID,
		QueryLogLevel level, String message, Map<String, Object> metadata) {
		// no-op for non-AWS implementations
	}

	/**
	 * Convenience overload that logs at INFO level with no metadata.
	 */
	public default void logQueryEvent(String queryID, String workerID, String message) {
		logQueryEvent(queryID, workerID, QueryLogLevel.INFO, message, null);
	}
}
