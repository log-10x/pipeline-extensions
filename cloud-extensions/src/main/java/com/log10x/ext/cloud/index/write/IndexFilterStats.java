package com.log10x.ext.cloud.index.write;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

/**
 * Utility class for logging indexer filter stats.
 */
public class IndexFilterStats {

	private static final Logger logger = LogManager.getLogger(IndexFilterStats.class);

	private static final int KEY_LEN_MOD = 50;
	
	private final IntArrayList filterStats;
	
	private final Long2IntOpenHashMap epochHistogram;
	
	private final Int2IntOpenHashMap probHistogram;
	
	private final Int2IntOpenHashMap groupSizes;
	
	private final Int2IntOpenHashMap keySizes;
	
	private final Int2IntOpenHashMap elementCounts;

	public IndexFilterStats() {
						
		if (logger.isInfoEnabled()) {
			
			this.filterStats = new IntArrayList();
			this.epochHistogram = new Long2IntOpenHashMap();
			this.probHistogram = new Int2IntOpenHashMap();
			this.groupSizes = new Int2IntOpenHashMap();
			this.keySizes = new Int2IntOpenHashMap();
			this.elementCounts = new Int2IntOpenHashMap();
			
		} else {
			
			this.filterStats =  null;
			this.epochHistogram =  null;
			this.probHistogram = null;
			this.groupSizes = null;
			this.keySizes = null;
			this.elementCounts = null;
		}
	}
	
	public void addEpoch(long resolutionEpoch) {
		
		if (this.epochHistogram != null) {
			epochHistogram.addTo(resolutionEpoch, 1);
		}
	}
		
	public void addFilter(double filterProb, int byteLen, int elementSize) {
		
		if (this.filterStats != null) {
			
			int prob = (int)(filterProb * 100);
			
			filterStats.add(byteLen);
			filterStats.add(elementSize);
			filterStats.add(prob);
					
			probHistogram.addTo(prob, 1);
			
			int roundedBytes = (byteLen / KEY_LEN_MOD) * KEY_LEN_MOD;
			int roundedElements = (elementSize / KEY_LEN_MOD) * KEY_LEN_MOD;
			
			keySizes.addTo(roundedBytes, 1);
			elementCounts.addTo(roundedElements, 1);
		}
	}
	
	public void addFilterGroup(int size) {
		
		if (this.groupSizes != null) {
			this.groupSizes.addTo(size, 1);
		}
	}
	
	public void logStats() {
		
		if (!logger.isInfoEnabled()) {
			return;
		}
		
		int filterSize = filterStats.size() / 3;

		long totalBytes = 0;

		for (int i = 0; i < filterSize; i++) {	
			
			int bytes = filterStats.getInt(i) * 3;
			totalBytes += bytes;
		}
			
		StringBuilder message = new StringBuilder("index complete. Bytes: ").
			append(totalBytes).
			append(", filters.size: ").
			append(filterSize);
						
		List<Int2IntOpenHashMap.Entry> sortedProbHistogram = sortedEntries(probHistogram);
		
		List<Int2IntOpenHashMap.Entry> sortedKeySizes = sortedEntries(keySizes);
		
		List<Int2IntOpenHashMap.Entry> sortedElemCounts = sortedEntries(elementCounts);
		
		message.
			append(", probability historgram: ").
			append(sortedProbHistogram).
			append(", key sizes: ").
			append(sortedKeySizes).
			append(", element counts: ").
			append(sortedElemCounts).
			append(", epochs: ").
			append(epochHistogram.size());
		
		if (logger.isDebugEnabled()) {
			
			List<Int2IntOpenHashMap.Entry> sortedGroupSizes = sortedEntries(groupSizes);
			
			List<Long2IntOpenHashMap.Entry> sortedEpochHistogram = sortedEntries(epochHistogram);
			
			message.
				append(", group sizes: ").
				append(sortedGroupSizes).
				append(", epoch histogram: ").
				append(sortedEpochHistogram).
				append(", filters: ");				
			
			for (int i = 0; i < filterSize; i++) {
				
				if (i > 0) {
					message.append(',');
				}
				
				int bytes = filterStats.getInt(i) * 3;
				int elems = filterStats.getInt(i) * 3 + 1;
				int prob  = filterStats.getInt(i) * 3 + 2;
				
				message.
					append("[bytes: ").
					append(bytes).
					append(", elems: ").
					append(elems).
					append(", prob: ").
					append(prob).
					append("]");
			}
		}
		
		logger.info(message);
	}
	
	private static List<Int2IntOpenHashMap.Entry> sortedEntries(Int2IntOpenHashMap map) {
		
		List<Int2IntOpenHashMap.Entry> result = new ArrayList<>(map.int2IntEntrySet());
		
		result.sort(new Comparator<Int2IntOpenHashMap.Entry>() {

			@Override
			public int compare(Int2IntOpenHashMap.Entry o1, Int2IntOpenHashMap.Entry o2) {
				return Integer.compare(o1.getIntKey(), o2.getIntKey());
			}
		});
		
		return result;
	}
	
	private static List<Long2IntOpenHashMap.Entry> sortedEntries(Long2IntOpenHashMap map) {
		
		List<Long2IntOpenHashMap.Entry> result = new ArrayList<>(map.long2IntEntrySet());
		
		result.sort(new Comparator<Long2IntOpenHashMap.Entry>() {

			@Override
			public int compare(Long2IntOpenHashMap.Entry o1, Long2IntOpenHashMap.Entry o2) {
				return Long.compare(o1.getLongKey(), o2.getLongKey());
			}
		});
		
		return result;
	}
}
