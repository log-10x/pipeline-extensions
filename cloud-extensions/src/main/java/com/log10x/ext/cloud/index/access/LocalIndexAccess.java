package com.log10x.ext.cloud.index.access;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.options.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.options.ObjectStorageAccessOptions;
import com.log10x.ext.cloud.index.interfaces.options.ObjectStorageInputContainerOptions;
import com.log10x.ext.cloud.index.util.stream.ByteRangeFileInputStream;

/**
 * Implementation of the {@link ObjectStorageIndexAccessor} interface over
 * a file system. Primarily used for debugging purposes of remote KV storages
 * locally.
 * 
 */
public class LocalIndexAccess implements ObjectStorageIndexAccessor {

	/**
	 * Defines an object stored during the indexing of an input object within an
	 * underlying KV storage (e.g. AWS S3). Index objects include JSON serialized
	 * tenxTemplate values and encoded bloom filters.
	 * 
	 * To learn more see https://doc.log10x.com/run/input/objectStorage/index/#storage-filters 
	 */
	protected static class IndexObject {

		/**
		 * the key value of this index object
		 */
		public final String name;

		/**
		 * optional metadata tags
		 */
		public final Map<String, String> tags;

		protected IndexObject(String name, Map<String, String> tags) {
			this.name = name;
			this.tags = tags;
		}
		
		protected IndexObject() {
			this(null, Collections.emptyMap());
		}

		@Override
		public int hashCode() {
			return name.hashCode();
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof IndexObject)) {
				return false;
			}

			if (this == obj) {
				return true;
			}

			IndexObject other = (IndexObject) obj;

			return ((Objects.equals(this.name, other.name)) &&
					(tags.equals(other.tags)));
		}

		@Override
		public String toString() {
			return String.format("%s tags: %s", name, tags);
		}
	}
	
	protected final ObjectStorageAccessOptions options;
	
	private final Object objectLock;
	
	private final boolean debugMode;
	
	private final static MessageDigest md;
						
	private volatile File indexFolder;
	
	private Map<String, List<String>> indexCache;
	
	private ConcurrentHashMap<String, String> indexKeysCache;
	
	public LocalIndexAccess(ObjectStorageAccessOptions options, EvaluatorBean evaluatorBean) {
		
		this.options = options;		
		this.objectLock = new Object();
		
		Object raw = evaluatorBean.env("TENX_DEBUG_INDEX_ACCESS", Boolean.FALSE);

		if (raw instanceof Boolean bool) {
			this.debugMode = bool.booleanValue();
		} else if (raw instanceof String s) {
			this.debugMode = Boolean.valueOf(s);
		} else {
			this.debugMode = false;
		}
	}
	
	private void initializeIndexFolder() {
		
		if (this.indexFolder != null) {
			return;
		}
		
		synchronized (this.objectLock) {
			
			if (this.indexFolder != null) {
				return;
			}
			
			if (options.indexContainer() == null) {
				throw new IllegalStateException();
			}
			
			String indexContainer = options.indexContainer();
			
			File indexContainerFile = new File(indexContainer); 
			
			if (!indexContainerFile.isDirectory()) {
				throw new IllegalArgumentException("'indexContainer' must be a folder, received: " + indexContainer);
			}
			
			this.indexCache = new ConcurrentHashMap<>();
			this.indexKeysCache = new ConcurrentHashMap<>();
			
			this.indexFolder = indexContainerFile;
		}
	}
	
	private static String hash(String name) {
		return Base64.getUrlEncoder().encodeToString(md.digest(name.getBytes()));
	}
		
	@Override
	public String putObject(String prefix, String key, InputStream content,
		long contentLength, Map<String, String> tags) throws IOException {
				
		if ((key == null) || 
			(key.isBlank())) {
			
			throw new IllegalArgumentException(prefix + ": " + contentLength + ": " + tags);
		}
		
		this.initializeIndexFolder();
		
		String result;
		
		if (content != null) {
			
			result = String.join(this.keyPathSeperator(), prefix, key);
			
			if (!this.debugMode) {
				
				File file = new File(this.indexFolder, result);
				
				file.getParentFile().mkdirs();
						
				Files.copy(content, file.toPath(), 
					StandardCopyOption.REPLACE_EXISTING);
			}
			
		} else {
			
			String hash = hash(key);
			
			result = String.join(this.keyPathSeperator(), prefix, hash);

			if (!this.debugMode) {
				
				File file = new File(this.indexFolder, result);
				
				file.getParentFile().mkdirs();
							
				try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
					
					MapperUtil.jsonMapper.writeValue(writer, 
						new IndexObject(key, tags));
				}
			}
		}

		return result;
	}
	
	@Override
	public InputStream readObject(String key, 
		long off, int len) throws IOException {
		
		ObjectStorageInputContainerOptions options = (ObjectStorageInputContainerOptions)this.options;
		
		File targetObjectFile = new File(options.inputContainer(), key);
		
		return (off >= 0) && (len > 0) ?
			new ByteRangeFileInputStream(targetObjectFile, off, len) :
			new FileInputStream(targetObjectFile);
	}
	
	@Override
	public InputStream readObject(String key) throws IOException {
		
		this.initializeIndexFolder();

		File indexObjectFile = new File(this.indexFolder, key);
				
		try (FileInputStream fis = new FileInputStream(indexObjectFile)) {
			
			return new ByteArrayInputStream(fis.readAllBytes());
		}
	}
	
	private List<String> prefixFiles(String prefix) throws IOException {
					
		List<String> existing = indexCache.get(prefix);
			
		if (existing != null) {
			return existing;
		}
		
		synchronized (this) {

			existing = indexCache.get(prefix);
			
			if (existing != null) {
				return existing;
			}
			
			IndexContainerOptions storageOptions = (IndexContainerOptions)this.options;
			
			String indexContainer = storageOptions.indexContainer();
			
			Path targetPath = Paths.get(String.join(this.keyPathSeperator(),
				indexContainer, prefix));
			
			if (!targetPath.toFile().isDirectory()) {
				return Collections.emptyList();
			}
			
			List<String> result = new ArrayList<>();
			
			int copyFrom = (!indexContainer.endsWith(this.keyPathSeperator())) ?
				indexContainer.length() + this.keyPathSeperator().length() :
				indexContainer.length();
				
			Files.find(targetPath, 999, (p, bfa) -> bfa.isRegularFile())
				.forEach(new Consumer<Path>() {
					
				    @Override
					public void accept(Path curr) {
				    	
				    	File file = curr.toFile();
				    	
				    	if (!file.isHidden()) {			    		
					    	result.add(curr.toString().substring(copyFrom));
				    	}		    	
				    }
				});
			
			result.sort(Comparator.comparing(String::toString));
			
			indexCache.put(prefix, result);
			
			return result;
		}
	}

	@Override
	public Iterator<List<String>> iterateObjectKeys(String prefix, String searchAfter, boolean failOnMissing) throws IOException {
		
		this.initializeIndexFolder();
		
		List<String> files = this.prefixFiles(prefix);
		
		return (searchAfter != null) ? 
			iteratorAfter(files, prefix + this.keyPathSeperator() + searchAfter) :
			Collections.singleton(files).iterator();
		}
	
	private static Iterator<List<String>> iteratorAfter(List<String> paths, String searchAfter) {
					
		int index = Collections.binarySearch(paths, searchAfter, 
			Comparator.comparing(String::toString));
		
		int iterFrom = (index >= 0) ?
			index + 1 :
			Math.abs(index + 1);
					
		List<String> result = paths.subList(iterFrom, paths.size());
		
		return Collections.singleton(result).iterator();
	}
	
	@Override
	public String expandIndexObjectKey(String key) {
			
		String existing = indexKeysCache.get(key);
		
		if (existing != null) {
			return existing;
		}
		
		try {

			File keyFile = new File(key);
			String parent = keyFile.getParent();

			IndexObject indexObject = this.indexObject(key);
			
			String result = (parent != null) ?
				parent + this.keyPathSeperator() + indexObject.name :
				indexObject.name;	
			
			indexKeysCache.put(key, result);
			
			return result;
			
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	
	private IndexObject indexObject(String key) throws IOException {
		
		this.initializeIndexFolder();
		
		File indexObjectFile = new File(this.indexFolder, key);
		
		try (FileReader reader = new FileReader(indexObjectFile, StandardCharsets.UTF_8)) {
			
			return MapperUtil.jsonMapper.readValue(reader, 
				IndexObject.class);			
		}
	}
	
	@Override
	public int keyByteLength(CharSequence key) {
		return AWSIndexAccess.isValidS3Key(key);
	}
	
	@Override
	public String keyPathSeperator() {
		return File.separator;
	}
	
	static {
		
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public int deleteObjects(List<String> keys) {
		
		int result = 0;
		
		for (String key : keys) {
			
			File file = new File(this.indexFolder, key);
			
			if (file.delete()) {
				result++;
			}	
		}
		
		return result;
	}
}
