package com.log10x.ext.cloud.index.interfaces.options;

import java.util.Map;

/**
 * Interface for instantiating the needed index accessor from a fully qualified class name.
 *
 * The accessor is responsible for handling operations on KV storage (e.g. AWS S3),
 * additional pipeline invocations (e.g. AWS Lambda), and query event logging
 * (e.g. AWS CloudWatch Logs).
 */
public interface ObjectStorageAccessOptions {

	public Map<String, String> args();

	/**
	 *
	 * @return fully qualified class name implementing {@link ObjectStorageIndexAccessor}
	 */
	public String accessorAlias();

	/**
	 *
	 * @return name of container (e.g. AWS S3 bucket) in which index objects are stored.
	 * To learn more see https://doc.log10x.com/run/input/objectStorage/index/#storage-filters
	 */
	public String indexContainer();

	/**
	 *
	 * @return name of container (e.g. S3 bucket)
	 */
	public String inputContainer();

	/**
	 * @return the CloudWatch Logs log group for query event logging, or empty/null
	 * to disable query logging.
	 */
	public String queryLogGroup();
}
