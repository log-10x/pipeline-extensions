package com.log10x.ext.cloud.index.interfaces.options;

/**
 * Defines the options required to read an object from a target 
 * KV container (e.g. AWS S3 bucket) 
 */
public interface ObjectStorageInputContainerOptions extends IndexContainerOptions {

	/**
	 * @return name of the object stored within {@link #inputContainer()}
	 */
	public String inputObject();
}
