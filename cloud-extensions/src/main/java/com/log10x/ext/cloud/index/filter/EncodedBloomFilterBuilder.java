package com.log10x.ext.cloud.index.filter;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.log10x.ext.cloud.index.shared.TokenSplitter;
import com.signalfx.shaded.google.common.base.Utf8;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

/**
 * A utility class used to construct a set of UTF8 encoded bloom filters
 * from a set of strings, each matching a target filter error probability 
 * and not exceeding N bytes in length. 
 * 
 * This class in NOT thread-safe.
 */
public class EncodedBloomFilterBuilder {
		
	public static final boolean DEBUG = true;
		
	private final static int MIN_ITEMS = 10000;
	
	private final static int STEP_CHECK_FULL = 1000;
	
	private final static int MIN_BUFFER_SIZE = 1024;
				
	private final Set<String> valueSet;

	private final List<String> values;

	private final List<EncodedBloomFilter> encodedFilters;

	private final TokenSplitter tokenSplitter;

	private final int minFilterProbability;

	private CharBuffer charBuffer;

	protected long maxEpoch;

	protected long minEpoch;

	public EncodedBloomFilterBuilder(TokenSplitter tokenSplitter, int minFilterProbability) {

		this.tokenSplitter = tokenSplitter;
		this.minFilterProbability = minFilterProbability;
		this.valueSet = new HashSet<String>();
		this.encodedFilters = new ArrayList<>();
		this.values = new ArrayList<>();
	}
	
	public static BloomFilter<String> createBloomFilter(int expectedElements, double prob) {		
		return new FilterBuilder(expectedElements, prob).buildBloomFilter();
	}
			
	private BloomFilter<String> createBloomFilter(double prob, 
		int from, int to) {
				
		BloomFilter<String> result = createBloomFilter(to - from, prob);
	
		for (int i = from; i < to; i++) {

			result.add(values.get(i));
		}

		return result;
	}
		
	private String filterBytesToString(BloomFilter<String> filter) {
		
		byte[] filterBytes = filter.getBitSet().toByteArray();
			
		int neededCapacity = filterBytes.length + CodedBloomFilter.HEADER_SIZE;	
	
		if ((this.charBuffer == null) || 
			(charBuffer.capacity() < neededCapacity)) {
			
			int capacity = Math.max(neededCapacity, MIN_BUFFER_SIZE);
			this.charBuffer = CharBuffer.allocate(capacity);
			
		} else {
			
			charBuffer.clear();
		}
		
		int pos = charBuffer.capacity() - filterBytes.length - CodedBloomFilter.HEADER_SIZE;
	
		FilterBuilder config = filter.config();

		charBuffer.position(pos);
				
		charBuffer.put((char)(config.falsePositiveProbability() * 100));
		charBuffer.put((char)(config.expectedElements()));
		
		for (int i = 0; i < filterBytes.length; i++) {
			
			byte b = filterBytes[i];
			char c = CodedBloomFilter.encode(b);
			
			charBuffer.put(c);
		}	
		
		charBuffer.position(pos);
		
		return charBuffer.toString();
	}
	
	protected int filterByteLen(String filter) {
		return Utf8.encodedLength(filter);
	}
	
	private void encodeBloomFilter(int from, int to) throws IOException {
		
		int prob = 1;
				
		while (prob < 100) {
		
			BloomFilter<String> filter = this.createBloomFilter((double)prob / 100,
				from, to);
					
			String encodedFilter = this.filterBytesToString(filter);
													
			int encodedFilterByteLen = this.filterByteLen(encodedFilter);
				
			if (encodedFilterByteLen != -1) {
				
				encodedFilters.add(new EncodedBloomFilter((double)prob / 100, 
					encodedFilter, to - from,  encodedFilterByteLen, 
					(DEBUG) ? new ArrayList<>(values.subList(from, to)) : null));
					
				return;
				
			} else {
								
				if (prob == this.minFilterProbability) {
					
					int size   = to - from;
					int middle = from + (size / 2);
					
					this.encodeBloomFilter(from, middle);
					this.encodeBloomFilter(middle, from + size);
					
					return;	
				}
				
				prob++;
			}	
		}		
	}

	private void appendValue(Object value, boolean applyDelimiters) {

		if (value == null) {
			return;
		}

		if (value instanceof List<?>) {

			for (Object item : ((List<?>) value)) {
				this.appendValue(item, applyDelimiters);
			}

		} else if (value instanceof Map<?, ?>) {

			for (Object item : ((Map<?, ?>) value).values()) {

				this.appendValue(item, applyDelimiters);
			}

		} else {
			String str = value.toString();

			if ((str == null) ||
				(str.isBlank())) {
				return;
			}

			if (applyDelimiters) {
				this.tokenSplitter.fill(str, valueSet);
			} else {
				this.valueSet.add(str);
			}
		}
	}

	public void append(EncodedEventInput output, long epoch) throws IOException {
		
		this.maxEpoch = (this.maxEpoch > 0) ?
			Math.max(this.maxEpoch, epoch) :
			epoch;

		this.minEpoch = (this.minEpoch > 0) ?
			Math.min(this.minEpoch, epoch) :
			epoch;
		
		this.appendValue(output.templateHash, false);
				
		if (output.vars != null) {
			
			for (Object var : output.vars) {
				this.appendValue(var, false);	
			}
		}
		
		if (output.enrichmentFields != null) {
		
			for (Object enrichmentValue : output.enrichmentFields.values()) {
				
				this.appendValue(enrichmentValue, true);
			}
		}
				
		int size = valueSet.size();
		
		int stepCheck = (size - MIN_ITEMS) % STEP_CHECK_FULL;
		
		if ((size > 0) && 
			(stepCheck == 0)) {	
			
			this.appendPendingBloomFilter();
		} 
	}

	private void appendPendingBloomFilter() throws IOException {
				
		values.addAll(this.valueSet);

		try {
			
			this.encodeBloomFilter(0, values.size());
			
		} finally {
		
			valueSet.clear();
			values.clear();
		}
	}
	
	public List<EncodedBloomFilter> build() throws IOException {
		
		if (!valueSet.isEmpty()) {
			this.appendPendingBloomFilter();
		} 
		
		return this.encodedFilters;
	}
	
	@Override
	public String toString() {
		return encodedFilters.toString();
	}	
}