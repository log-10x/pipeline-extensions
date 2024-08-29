package com.log10x.ext.cloud.index.interfaces;

/**
 * Defines a container and object pair within an underlying Object storage
 */
public interface IndexContainerOptions extends ObjectStorageAccessOptions {
			
	/**
	 * 
	 * @return prefix under which index objects are stored in {@link indexContainer}
	 */
	public String prefix();
}
