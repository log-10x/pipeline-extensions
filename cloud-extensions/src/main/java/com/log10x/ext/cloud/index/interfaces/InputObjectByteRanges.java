package com.log10x.ext.cloud.index.interfaces;

/**
 * Defines a set of byte ranges pairs used to fetch target segments from
 * an object within an underlying KV storage. To learn more about byte range fetches, see:
 * {@link https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html} 
 */
public interface InputObjectByteRanges {
	
	/**
	 * 
	 * @return number of offset and length pairs.
	 */
	public int size();
	
	/**
	 * Returns a target byte range offset value
	 * 
	 * @param 	index of offset
	 * 
	 * @return 	offset value at {@code index} position
	 */
	public long offset(int index); 
	
	/**
	 * Returns a target byte range length value
	 * 
	 * @param 	index of length
	 * 
	 * @return 	length value at {@code index} position
	 */
	public int length(int index);
		
	/**
	 * appends a offset length pair to this list
	 * 
	 * @param 	offset	
	 * 			within byte range
	 * 	
	 * @param	length	
	 * 			within byte range
	 */
	public void add(long offset, int length);
	
	/**
	 * 
	 * @return array of offset and length pairs
	 */
	public long[] byteRanges();	
}
