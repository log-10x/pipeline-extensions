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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.IndexObject;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageAccessOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageInputContainerOptions;
import com.log10x.ext.cloud.index.util.stream.ByteRangeFileInputStream;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * Implementation of the {@link ObjectStorageIndexAccessor} interface over
 * a file system. Primarily used for debugging purposes of remote KV storages
 * locally.
 * 
 */
public class LocalIndexAccess implements ObjectStorageIndexAccessor {
	
	protected final ObjectStorageAccessOptions options;
	
	private final Object objectLock;
	
	private final static MessageDigest md;

	protected static class FileAccessorIndexObject extends IndexObject {
		
		protected final String value;
		
		public FileAccessorIndexObject() {
			this(null, Collections.emptyMap(), null);
		}
		
		public FileAccessorIndexObject(String name, Map<String, String> tags, String value) {
			
			super(name, tags);
			this.value = value;
		}
	}
						
	private volatile File indexFolder;
	
	private Map<String, List<String>> indexCache;
	
	private ConcurrentHashMap<String, String> indexKeysCache;
	
	public LocalIndexAccess(ObjectStorageAccessOptions options) {
		
		this.options = options;		
		this.objectLock = new Object();
	}
	
	private void initializeIndex() {
		
		if (this.indexFolder != null) {
			return;
		}
		
		synchronized (this.objectLock) {
			
			if (this.indexFolder != null) {
				return;
			}
			
			File indexContainer = new File(((IndexContainerOptions)options).indexContainer()); 
			
			if (!indexContainer.isDirectory()) {
				throw new IllegalArgumentException("'indexContainer' must be a folder for: " + options);
			}
			
			this.indexCache = new ConcurrentHashMap<>();
			this.indexKeysCache = new ConcurrentHashMap<>();
			
			this.indexFolder = indexContainer;
		}
	}
	
	private static String hash(String name) {
		return Base64.getUrlEncoder().encodeToString(md.digest(name.getBytes()));
	}
		
	@Override
	public String putObject(String prefix, String key, String value,
		Map<String, String> tags) throws IOException {
				
		if ((key == null) || 
			(key.isBlank())) {
			
			throw new IllegalArgumentException(prefix + ": " + value + ": " + tags);
		}
		
		this.initializeIndex();
		
		String path = String.join(this.keyPathSeperator(), prefix, key);
		File indexObjectFile = new File(this.indexFolder, path);
		
		indexObjectFile.getParentFile().mkdirs();
					
		try (FileWriter writer = new FileWriter(indexObjectFile, StandardCharsets.UTF_8)) {
			writer.write(value);
		}
		
		return path;
	}
	
	@Override
	public String putIndexObject(String prefix, String key, String value,
		Map<String, String> tags) throws IOException {
				
		if ((key == null) || 
			(key.isBlank())) {
			
			throw new IllegalArgumentException(prefix + ": " + value + ": " + tags);
		}
		
		this.initializeIndex();
		
		String hash = hash(key);
		
		String path = String.join(this.keyPathSeperator(), prefix, hash);
		File indexObjectFile = new File(this.indexFolder, path);
		
		indexObjectFile.getParentFile().mkdirs();
					
		try (FileWriter writer = new FileWriter(indexObjectFile, StandardCharsets.UTF_8)) {
			
			MapperUtil.jsonMapper.writeValue(writer, 
				new FileAccessorIndexObject(key, tags, value));
		}
		
		return path;
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
		
		this.initializeIndex();

		File indexObjectFile = new File(this.indexFolder, key);
				
		try (FileInputStream fis = new FileInputStream(indexObjectFile)) {
			
			return new ByteArrayInputStream(fis.readAllBytes());
		}
	}
	
	@Override
	public InputStream readIndexObject(String key) throws IOException {
		
		this.initializeIndex();

		File indexObjectFile = new File(this.indexFolder, key);
				
		try (FileReader reader = new FileReader(indexObjectFile)) {
			
			FileAccessorIndexObject object = MapperUtil.jsonMapper.readValue(reader, 
				FileAccessorIndexObject.class);
			
			return new ByteArrayInputStream(object.value.getBytes());
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
	public Iterator<List<String>> iterateObjectKeys(String prefix, 
		String searchAfter) throws IOException {
		
		this.initializeIndex();
		
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
		
		this.initializeIndex();
		
		File indexObjectFile = new File(this.indexFolder, key);
		
		try (FileReader reader = new FileReader(indexObjectFile, StandardCharsets.UTF_8)) {
			
			return MapperUtil.jsonMapper.readValue(reader, 
				FileAccessorIndexObject.class);			
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
}
