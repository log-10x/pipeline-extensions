package com.log10x.ext.cloud.index.interfaces.options;

/**
 * Defines a filter to apply when reading events from a byte range within 
 * an input object
 */
public interface IndexQueryRequest {

	/** 
	 * 
	 * @return	logical name of the query
	 */
	public String name();
	
	/**
	 * 
	 * @return	filter events must pass
	 */
	public String filter();
	
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
	
	/**
	 * @return	identify the query unique ID
	 */
	public String ID();
	
	/**
	 * @return	epoch of when query should cease processing
	 */
	public long elapseTime();
}
