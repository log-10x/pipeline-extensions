package com.log10x.ext.cloud.index.filter;

import java.util.Collection;

/**
 * Data structure for describing a bloom filter encoded to UFT8
 */
public class EncodedBloomFilter extends CodedBloomFilter {

	/**
	 * error probability
	 */
	public final double prob;
		
	/**
	 * number of elements
	 */
	public final int elementSize;
	
	/**
	 * size when encoded to UTF8
	 */
	public final int byteLen;
		
	/**
	 * values contained. For debug purposes only.
	 */
	public final Collection<String> values;
	
	public EncodedBloomFilter(double prob, String value, int byteLen,
		int elementSize, Collection<String> values) {
		
		super(value);
		this.prob = prob;
		this.elementSize = elementSize;
		this.values = values;
		this.byteLen = byteLen;
	}
	
	@Override
	public String toString() {
		
		return String.format("prob: %f, elems: %d, byteLen: %d filter: %s", 
			prob, elementSize, byteLen, encodedFilter);
	}
}
