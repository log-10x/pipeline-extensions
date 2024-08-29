package com.log10x.ext.cloud.index.interfaces;

/**
 * Defines a request to read a byte range from an input object 
 * residing within an underlying storage and extract events matching 
 * a target timerange and search term list.
 */
public interface QueryObjectRequest extends 
	ObjectStorageInputContainerOptions, 
	InputObjectByteRanges,
	IndexQueryRequest {

	
}
