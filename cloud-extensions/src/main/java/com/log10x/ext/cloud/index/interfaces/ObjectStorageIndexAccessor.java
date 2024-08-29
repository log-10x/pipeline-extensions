package com.log10x.ext.cloud.index.interfaces;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * This interface provides a simple abstraction over a the different services
 * needed for 'L1x Object Storage Analyzer.
 * 
 * Those services include access to KV value storage and additional invocations
 * of the 'l1x pipeline'.
 * 
 * For storage access see {@link com.log10x.l1x.index.interfaces.ObjectStorageAccessor}
 * For pipeline invocations see {@link com.log10x.l1x.index.interfaces.PipelineInvocationAccessor}
 * 
 * For an AWS implementation, see: {@link com.log10x.l1x.index.access.AWSIndexAccess}
 */
public interface ObjectStorageIndexAccessor extends PipelineInvocationAccessor, ObjectStorageAccessor {

	// Index object tags
	
	/*
	 * The string values added to the index object's bloom filters. For debug
	 * purposes only.
	 */
	public static final String VALUES_TAG = "values";
	
	// l1x data (templates/reverse index) key path prefixes

	/**
	 * the top level prefix under which l1x data objects are stored 
	 */
	public static final String L1X_PREFIX = "l1x";
	
	/**
	 * the 2nd to top level prefix under which l1x reverse index objects are stored 
	 */
	public static final String META_INDEX_PREFIX = "mindex";
	
	/**
	 * the 2nd to top level prefix under which l1x reverse index objects are stored 
	 */
	public static final String REVERSE_INDEX_PREFIX = "rindex";
	
	/**
	 * the 2nd to top level prefix under which l1x templates objects are stored 
	 */
	public static final String TEMPLATE_PREFIX = "templ";
	
	/**
	 * the 3rd to top prefix to use l1x templates objects whose value can be inlined into as the key value
	 * (e.g. smaller than 1024) bytes are stored. For example:
	 * /l1x/templ/inline/<template-json>
	 */
	public static final String TEMPLATE_INLINE_PREFIX = "inline";
	
	/**
	 * the 3rd to top prefix to use l1x templates objects whose value can not be inlined
	 * into as the key value (e.g. greater than 1024) bytes are stored. For example:
	 * /l1x/templ/enclose/<template-hash>
	 */
	public static final String TEMPLATE_ENCLOSED_PREFIX = "enclose";

	/**
	 * Returns the path under which to store enclosed l1x template JSON objects.
	 * 
	 * @param 	prefix 
	 * 			app prefix
	 * 
	 * @return	the path within the underlying object storage under which
	 * 			enclosed (i.e. whose length is greater than {@link #keyByteLength})
	 * 			are stored.
	 */
	public default String templateEnclosedPath(String prefix) {
		
		return String.join(this.keyPathSeperator(),
			templatePath(prefix),
			TEMPLATE_ENCLOSED_PREFIX
		);
	}

	/**
	 * Returns the path under which to store inlined l1x template JSON objects.
	 * 
	 * @param 	prefix 
	 * 			app prefix
	 * 
	 * @return	the path within the underlying object storage under which
	 * 			inline (i.e. whose JSON escaped length is smaller than {@link #keyByteLength})
	 * 			are stored.
	 */
	public default String templateInlinePath(String prefix) {
		
		return String.join(this.keyPathSeperator(),
			templatePath(prefix),
			TEMPLATE_INLINE_PREFIX
		);
	}

	/**
	 * Returns the path under which from which to read l1x template objects.
	 * 
	 * @param 	prefix 
	 * 			app prefix
	 * 
	 * @return	the path within the underlying object storage under which
	 * 			l1x template objects are stored.
	 */
	public default String templatePath(String prefix) {
		
		return String.join(this.keyPathSeperator(),
			L1X_PREFIX, 
			prefix,
			TEMPLATE_PREFIX
		);
	}
	
	/**
	 * Returns the path under which from which to read l1x meta index objects.
	 * 
	 * @param 	prefix 
	 * 			app prefix
	 * 
	 * @return	the path within the underlying object storage under which
	 * 			l1x meta index objects are stored.
	 */
	public default String metaIndexPath(String prefix) {
		
		return String.join(this.keyPathSeperator(),
			L1X_PREFIX, 
			prefix,
			META_INDEX_PREFIX
		);
	}
	
	/**
	 * Returns the path under which from which to read l1x reverse index objects.
	 * 
	 * @param 	prefix 
	 * 			app prefix
	 * 
	 * @return	the path within the underlying object storage under which
	 * 			l1x reverse index objects are stored.
	 */
	public default String reverseIndexPath(String prefix) {
		
		return String.join(this.keyPathSeperator(),
			L1X_PREFIX, 
			prefix,
			REVERSE_INDEX_PREFIX
		);
	}
	
	/**
	 * Translates a value to a key that can be used within the underlying storage.
	 * 
	 * @param 	key	
	 * 			value to translate
	 
	 * @return	translated key
	 */
	public default String expandIndexObjectKey(String key) {
		return key;
	}
	
	/**
	 * Reads the contents of an index object from the underlying object storage.
	 * 	 
	 * @param 	key
	 * 			the key value of the KV pair within the underlying storage 
	 * 				 
	 * @return 	an InputStream from which the index object can be read.
	 * 	 
	 * @throws 	IOException if 'key' cannot be read
	 */
	public default InputStream readIndexObject(String key) throws IOException {
		return readObject(key);
	}
	
	/**
	 * Stores an index object in the underlying storage
	 * 
	 * @param 	prefix
	 * 			value prefixed to {@code key} which constitutes the full key name
	 * 			under which the object is stored.
	 
	 * @param 	key
	 * 			the key to use when storing the KV pair.
	 * 
	 * @param 	value
	 * 			value to store
	 *
	 * @param 	tags
	 * 			tag values to associate with the underlying KV pair to be stored.
	 * 
	 * @return 	the path within the underlying storage to which the object was written to.
	 * 
	 * @throws IOException if the value could not be stored
	 */
	public default String putIndexObject(String prefix, String key, String value, Map<String, String> tags) throws IOException {
		return putObject(prefix, key, value, tags);
	}
}
