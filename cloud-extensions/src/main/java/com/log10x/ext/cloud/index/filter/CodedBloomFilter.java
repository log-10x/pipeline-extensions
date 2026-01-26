package com.log10x.ext.cloud.index.filter;

/**
 * Base class for encoded / decoded bloom filters
 */
public abstract class CodedBloomFilter {

	/**
	 * each bloom filters encodes its probability and size followed
	 * by the filter bitset.
	 */
	public static final int HEADER_SIZE = 2;
	
	public final String encodedFilter;

	public CodedBloomFilter(String encodedFilter) {
		this.encodedFilter = encodedFilter;
	}
	
	// utility methods for converting byte to char in an efficient way for UTF8
	
	public static char encode(byte b) {
		
		return (b > 0) ? 
			(char)b : 
			(char)(128 + Math.abs(b));
	}
	
	public static byte decode(char c) {
		
		return (c > 127) ? 
			(byte)(128 - c) :
			(byte)c;
	}
}
