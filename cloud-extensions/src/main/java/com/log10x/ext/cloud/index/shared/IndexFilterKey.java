package com.log10x.ext.cloud.index.shared;

import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;

/**
 * A bloom filter generated when indexing a target blob
 * To learn more @see <a href="https://doc.log10x.com/run/input/objectStorage/index/#storage-filters>index objects</a>
 */
public class IndexFilterKey {

	/**
	 * the key prefix, used to identify the app the object belongs to
	 */
	public final String prefix;

	/**
	 * the epoch part of the key, used to identify the time slice the key belongs to
	 */
	public final long epoch;

	/**
	 * a hash of the name of the target indexed object (i.e log file) used to reduce
	 * access to the underlying storage
	 */
	public final String targetHash;

	/**
	 * index of the byte-range within the target indexed object where this indexed
	 * filter is pointing to
	 */
	public final int byteRangeIndex;

	/**
	 * the encoded bloom filter for the object
	 */
	public final String encodedFilter;

	protected IndexFilterKey(String prefix, long epoch, String targetHash, 
		int byteRangeIndex, String encodedFilter) {

		this.prefix = prefix;
		this.epoch = epoch;
		this.targetHash = targetHash;
		this.byteRangeIndex = byteRangeIndex;
		this.encodedFilter = encodedFilter;
	}

	public String formatPath(ObjectStorageIndexAccessor accessor) {

		return String.join(accessor.keyPathSeperator(),
			this.prefix, String.valueOf(this.epoch),
			this.targetHash, String.valueOf(this.byteRangeIndex)
		);
	}
	
	@Override
	public String toString() {
		
		return String.format("prefix: %s, epoch: %d, targetHash: %s, byteRangeIndex: %d",
			this.prefix, this.epoch, this.targetHash, this.byteRangeIndex
		);
	}
	
	public static IndexFilterKey from(String prefix, long epoch,
		String targetHash, int byteRangeIndex, String encodedFilter) {

		return new IndexFilterKey(prefix, epoch, targetHash, byteRangeIndex, encodedFilter);
	}
	
	/**
	 * Returns a structured key object from the provided string key
	 * 
	 * @param	accessor
	 * 			ObjectStorageIndexAccessor responsible for handling this key
	 * 
	 * @param 	prefix 
	 * 			app prefix
	 * 
	 * @param	key
	 * 			raw string key
	 *  
	 * @return	structured index filter
	 */
	public static IndexFilterKey parseKey(ObjectStorageIndexAccessor accessor, String prefix, String key) {
		
		int prefixPos = key.indexOf(prefix);
		
		if (prefixPos == -1) {
			throw new IllegalStateException("Missing prefix - " + prefix + ", in key - " + key);
		}
		
		String keyPathSeperator = accessor.keyPathSeperator();
		
		int appPrefixLen = prefix.length() + keyPathSeperator.length();
		
		int epochSepIndex = key.indexOf(keyPathSeperator, prefixPos + appPrefixLen);
		
		if (epochSepIndex == -1) {
			throw new IllegalStateException("missing epoch seperator: '" + keyPathSeperator + "' in key:" + key);
		}
		
		String epochStr = key.substring(prefixPos + appPrefixLen, epochSepIndex);
		
		long epoch = Long.valueOf(epochStr);
		
		int epochPrefixLen = prefixPos + appPrefixLen + epochStr.length() + keyPathSeperator.length();
		
		int targetHashSepIndex = key.indexOf(keyPathSeperator, epochPrefixLen);
		
		if (targetHashSepIndex == -1) {
			
			throw new IllegalStateException("missing location hash seperator: '" + 
				keyPathSeperator + "' in key:" + key);
		}
		
		String targetHash = key.substring(epochPrefixLen, targetHashSepIndex);
		
		int targetHashPrefixLen = epochPrefixLen + targetHash.length() + keyPathSeperator.length();
		
		int byteRangeIndexSepIndex = key.indexOf(keyPathSeperator, targetHashPrefixLen);
		
		if (byteRangeIndexSepIndex == -1) {
			
			throw new IllegalStateException("missing byte range index seperator: '" + 
				keyPathSeperator + "' in key:" + key);
		}
		
		String byteRangeIndexStr = key.substring(targetHashPrefixLen, byteRangeIndexSepIndex);
		
		int byteRangeIndex = Integer.valueOf(byteRangeIndexStr);
			
		int byteRangeIndexPrefixLen = targetHashPrefixLen + byteRangeIndexStr.length() + keyPathSeperator.length();
		
		String expandedFilterKey = accessor.expandIndexObjectKey(key);
		
		String encodedFilter = expandedFilterKey.substring(byteRangeIndexPrefixLen);
		
		return new IndexFilterKey(prefix, epoch, targetHash, 
			byteRangeIndex, encodedFilter);
	}
	
	/**
	 * Returns the epoch value for an index fiter key
	 * 
	 * @param	accessor
	 * 			ObjectStorageIndexAccessor responsible for handling this key
	 * 
	 * @param 	prefix 
	 * 			app prefix
	 * 
	 * @param	key
	 * 			raw string key
	 *  
	 * @return	start epoch for the values encoded into this bloom filter
	 */
	public static long parseEpoch(ObjectStorageIndexAccessor accessor, String prefix, String key) {
		
		int prefixPos = key.indexOf(prefix);
		
		if (prefixPos == -1) {
			throw new IllegalStateException("Missing prefix - " + prefix + ", in key - " + key);
		}
		
		String keyPathSeperator = accessor.keyPathSeperator();
		
		int appPrefixLen = prefix.length() + keyPathSeperator.length();
		
		int epochSepIndex = key.indexOf(keyPathSeperator, prefixPos + appPrefixLen);
		
		if (epochSepIndex == -1) {
			throw new IllegalStateException("missing epoch seperator: '" + keyPathSeperator + "' in key:" + key);
		}
		
		String epochStr = key.substring(prefixPos + appPrefixLen, epochSepIndex);
		
		return Long.valueOf(epochStr);
	}
}
