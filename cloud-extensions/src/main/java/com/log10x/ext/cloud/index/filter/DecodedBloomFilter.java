package com.log10x.ext.cloud.index.filter;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.memory.BloomFilterMemory;

/**
 * A deserialized bloom filter read from a UTF encoded string.
 */
public class DecodedBloomFilter extends CodedBloomFilter {

	public final BloomFilter<String> filter;
		
	@SuppressWarnings("rawtypes")
	public DecodedBloomFilter(String encodedFilter) {
		
		super(encodedFilter);
			
		int filterByteLen = encodedFilter.length() - HEADER_SIZE;
		
		byte[] bytes = new byte[filterByteLen];
		
		for (int i = 0; i < filterByteLen; i++ ) {
			
			char c = encodedFilter.charAt(HEADER_SIZE + i);
			byte b = decode(c);
			
			bytes[i] = b;
		}
		
		double prob =  (double)encodedFilter.charAt(0) / 100;
		int expectedElements = encodedFilter.charAt(1);
		
		this.filter = EncodedBloomFilterBuilder.createBloomFilter(expectedElements, prob);
		
		((BloomFilterMemory)this.filter).setBitSet(BitSet.valueOf(
			ByteBuffer.wrap(bytes, 0, bytes.length)));
	}
	
	public boolean testAny(Collection<String> values) {
		
		for (String value : values) {
			
			if (this.test(value)) {
				return true;
			}			
		}
		
		return false;		
	}
	
	public boolean testAll(Collection<String> values) {
		
		for (String value : values) {
			
			if (!this.test(value)) {
				return false;
			}			
		}
		
		return true;		
	}
	
	public boolean test(String value) {		
		return filter.contains(value);				
	}
}
