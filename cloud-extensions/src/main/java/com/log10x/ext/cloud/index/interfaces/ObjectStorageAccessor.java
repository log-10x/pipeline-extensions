package com.log10x.ext.cloud.index.interfaces;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This interface provides a simple abstraction over an object storage
 * that can be used to index and query tenxObjects from. Implementations
 * of this interface can cover local file systems, DBs or more commonly
 * a Cloud object storage such as AWS S3, Azure Blobs and GCP Cloud storage.
 * For an AWS S3 implementation see: {@AWSIndexAccess}
 */
public interface ObjectStorageAccessor extends Closeable {

	/**
	 * 
	 * Returns the encoded by length of a storage key value
	 * 
	 * @param 	key 
	 * 			a candidate for a key within a KV pair to be written to storage
	 
	 * @return  the number of bytes the key will take up within the target storage
	 * 			or -1 if the 'key' is too long. For example, AWS S3 and GCP Storage
	 *  		keys are limited to 1024 bytes in length.
	 */
	public int keyByteLength(CharSequence key);
	
	/**
	 * Reads the contents of an object from the underlying object storage.
	 * 	 
	 * @param 	key
	 * 			the key value of the KV pair within the underlying storage 
	 * 				 
	 * @return 	an InputStream from which the index object can be read.
	 * 	 
	 * @throws 	IOException if 'key' cannot be read
	 */
	public InputStream readObject(String key) throws IOException;
		
	/**
	 * Reads the keys of index objects stored within the underlying object storage.
	 * 
	 * @param 	prefix
	 * 			limits the response to keys that begin with the specified prefix.
	 * 
	 * @param 	searchAfter
	 * 			where to to start listing from. Can be any key in the container.
	 * 
	 * @param	failOnMissing
	 * 			If true, throw exception when the object path is missing entirely.
	 * 
	 * @return 	an iterator of index object names starting with {@code prefix}
	 * 			and greater than {@code searchAfter}
	 * 
	 * @throws IOException 	if key cannot be read
	 * 
	 */
	public Iterator<List<String>> iterateObjectKeys(String prefix, String searchAfter, boolean failOnMissing) throws IOException;
	
	/**
	 * Read a blob from the underlying object storage
	 * 
	 * @param 	key
	 * 			identifies the object to read from input
	 * 
	 * @param 	off
	 * 			Beginning of byte range to fetch. Specify -1 to read all.
	 * 
	 * @param 	len
	 * 			number of bytes to read. Specify -1 to read all.
	 * 
	 * @return  {@link InputStream} from which the byte range can be read.
	 * 
	 * @throws IOException	if the object could not be read
	 */
	public InputStream readObject(String key, long off, int len) throws IOException;
	
	/**
	 * The separator used to join values in order to form a full key
	 * within the underlying storage. For example, for an AWS S3 bucket or NIX FS this 
	 * would return '/', while for a WIN FS this would return '\'. 
	 * 
	 * @return the storage specific key separator to use
	 */
	public String keyPathSeperator();
	
	/**
	 * Stores an index object in the underlying storage
	 * 
	 * @param 	prefix
	 * 			value prefixed to {@code key} which constitutes the full key name
	 * 			under which the object is stored (e.g., path)
	 
	 * @param 	key
	 * 			the key to use when storing the KV pair.
	 * 
	 * @param 	content
	 * 			data to store
	 * 
	 * @param 	contentLength
	 * 			Byte length of content
	 *
	 * @param 	tags
	 * 			tag values to associate with the KV pair to store.
	 * 
	 * @return 	the path within the underlying storage to which the object was written to.
	 * 
	 * @throws IOException if the value could not be stored
	 */
	public String putObject(String prefix, String key, InputStream content, long contentLength,
		Map<String, String> tags) throws IOException;
	
	/**
	 * 
	 * Delete a target list of objects from storage
	 * 
	 * @param keys	objects to delete
	 * 			
	 * @return		number of objects deleted
	 */
	public int deleteObjects(List<String> keys);
}
