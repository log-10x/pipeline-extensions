package com.log10x.ext.cloud.index.interfaces;

/**
 * Defines a list of line break positions within a stream
 */
public interface InputStreamLinebreaks {

	/**
	 * 
	 * Deletes all line break values positions up to {@code index} 
	 * 
	 * @param 	index
	 * 			position up to which to delete values (exclusive)
	 */
	public void deleteTo(long index);
	
	/**
	 * 	Returns the N line break position within a stream
	 * 
	 * @param 	index
	 * 			line break index
	 * 
	 * @return	line break position
	 */
	public long pos(long index);
	
	/**
	 * Returns the position of the last byte read from stream + 1;
	 * 
	 * @return	end of stream.
	 */
	public long endPos();
}
