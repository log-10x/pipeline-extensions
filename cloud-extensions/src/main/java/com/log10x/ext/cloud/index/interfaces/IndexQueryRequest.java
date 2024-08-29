package com.log10x.ext.cloud.index.interfaces;

import java.util.List;

/**
 * Defines a filter to apply when reading events from a byte range within 
 * an input object
 */
public interface IndexQueryRequest {

	/**
	 * 
	 * @return	list of terms to search for
	 */
	public List<String> terms();
	
	/**
	 * 
	 * @return	epoch value from which to begin search (inclusive)
	 */
	public long from();

	/**
	 * 
	 * @return	epoch value at which to end search (exclusive)
	 */
	public long to();	
}
