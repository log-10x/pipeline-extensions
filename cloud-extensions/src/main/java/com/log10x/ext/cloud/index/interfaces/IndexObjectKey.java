package com.log10x.ext.cloud.index.interfaces;

/**
 * Defines the key of an object stored during the indexing of an input object.
 */
public class IndexObjectKey {

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
	public final long targetHash;

	/**
	 * index of the byte-range within the target indexed object where this indexed
	 * filter is pointing to
	 */
	public final int byteRangeIndex;

	/**
	 * the encoded bloom filter for the object
	 */
	public final String encodedFilter;

	public IndexObjectKey(String prefix, long epoch, long targetHash, int byteRangeIndex, String encodedFilter) {

		this.prefix = prefix;
		this.epoch = epoch;
		this.targetHash = targetHash;
		this.byteRangeIndex = byteRangeIndex;
		this.encodedFilter = encodedFilter;
	}

	public String path(ObjectStorageIndexAccessor accessor) {

		String result;

		if (prefix != null) {

			result = String.join(accessor.keyPathSeperator(),
					prefix, String.valueOf(epoch),
					String.valueOf(targetHash), String.valueOf(byteRangeIndex));
		} else {
			result = String.join(accessor.keyPathSeperator(),
					String.valueOf(epoch), String.valueOf(targetHash), String.valueOf(byteRangeIndex));
		}

		return result;
	}
	
	@Override
	public String toString() {
		return String.join("_",
				prefix, String.valueOf(epoch),
				String.valueOf(targetHash), String.valueOf(byteRangeIndex));
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
	 * @param	indexObjectKey
	 * 			raw string key
	 * 
	 * @param	expandFilter
	 * 			whether or not to also expand the actual filter encoded in the key
	 * 
	 * @return	structured index object key
	 */
	public static IndexObjectKey fromPath(ObjectStorageIndexAccessor accessor,
			String prefix, String indexObjectKey, boolean expandFilter) {
		
		String keyPathSeperator = accessor.keyPathSeperator();
		int appPrefixLen = prefix.length() + keyPathSeperator.length();
		
		int epochSepIndex = indexObjectKey.indexOf(keyPathSeperator, appPrefixLen);
		
		if (epochSepIndex == -1) {
			throw new IllegalStateException("missing epoch seperator: '" + keyPathSeperator + "' in key:" + indexObjectKey);
		}
		
		String epochStr = indexObjectKey.substring(appPrefixLen, epochSepIndex);
		long epoch = Long.valueOf(epochStr);
		
		int epochPrefixLen = appPrefixLen + epochStr.length() + keyPathSeperator.length();
		
		int targetHashSepIndex = indexObjectKey.indexOf(keyPathSeperator, epochPrefixLen);
		
		if (targetHashSepIndex == -1) {
			throw new IllegalStateException("missing location hash seperator: '" + keyPathSeperator + "' in key:" + indexObjectKey);
		}
		
		String targetHashStr = indexObjectKey.substring(epochPrefixLen, targetHashSepIndex);
		long targetHash = Long.valueOf(targetHashStr);
		
		int targetHashPrefixLen = epochPrefixLen + targetHashStr.length() + keyPathSeperator.length();
		
		int byteRangeIndexSepIndex = indexObjectKey.indexOf(keyPathSeperator, targetHashPrefixLen);
		
		if (byteRangeIndexSepIndex == -1) {
			throw new IllegalStateException("missing byte range index seperator: '" + keyPathSeperator + "' in key:" + indexObjectKey);
		}
		
		String byteRangeIndexStr = indexObjectKey.substring(targetHashPrefixLen, byteRangeIndexSepIndex);
		int byteRangeIndex = Integer.valueOf(byteRangeIndexStr);
		
		if (!expandFilter) {
			return new IndexObjectKey(prefix, epoch, targetHash, byteRangeIndex, null);
		}
		
		int byteRangeIndexPrefixLen = targetHashPrefixLen + byteRangeIndexStr.length() + keyPathSeperator.length();
		
		String expandedFilterKey = accessor.expandIndexObjectKey(indexObjectKey);
		
		String encodedFilter = expandedFilterKey.substring(byteRangeIndexPrefixLen);
		
		return new IndexObjectKey(prefix, epoch, targetHash, byteRangeIndex, encodedFilter);
	}
}
