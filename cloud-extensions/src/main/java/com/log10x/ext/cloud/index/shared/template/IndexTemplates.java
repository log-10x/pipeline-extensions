package com.log10x.ext.cloud.index.shared.template;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.util.FileUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;
import com.log10x.ext.cloud.index.shared.BaseIndexWriter;

public class IndexTemplates {

	private static final Logger logger = LogManager.getLogger(IndexTemplates.class);

	public static final String TEMPLATE_HASH_PREFIX = "{\"templateHash\":\"";

	public static final String TEMPLATE_HASH_POSTFIX = "\"";

	public static final String RAW_PREFIX = "raw";

	public static final String MERGED_PREFIX = "merged";

	public static final String KEY_SEPERATOR = "_";
	
	public final List<String> allTemplateKeys;

	public final List<String> mergedTemplateKeys;
	
	public final List<String> rawTemplateKeys;
	
	public final List<String> currTemplateKeys;

	public final String currMergedTemplateKey;

	public final ObjectStorageIndexAccessor indexAccessor;
	
	public final String prefix;
	
	private IndexTemplates(ObjectStorageIndexAccessor indexAccessor,
		String prefix,
		List<String> allTemplateKeys,
		List<String> currTemplateKeys,
		List<String> mergedTemplateKeys,
		List<String> rawTemplateKeys, String 
		latestMergedTemplateKey) {
		
		this.indexAccessor = indexAccessor;
		this.prefix = prefix;
		
		this.allTemplateKeys = allTemplateKeys;
		this.currTemplateKeys = currTemplateKeys;
		this.mergedTemplateKeys = mergedTemplateKeys;
		this.rawTemplateKeys = rawTemplateKeys;
		this.currMergedTemplateKey = latestMergedTemplateKey;
	}
	
	public static IndexTemplates scan(ObjectStorageIndexAccessor indexAccessor, 
		String prefix, boolean mergedOnly) throws IOException {
		
		String templatePath = indexAccessor.indexObjectPath(IndexObjectType.template, prefix);

		Iterator<List<String>> indexIter = indexAccessor.iterateObjectKeys(
			templatePath, (mergedOnly ? null : MERGED_PREFIX), false);
		
		List<String> allTemplateKeys = new ArrayList<>();
		
		while (indexIter.hasNext()) {
			allTemplateKeys.addAll(indexIter.next());
		}		
		
		List<String> rawTemplateKeys = new ArrayList<>(allTemplateKeys.size());
		List<String> mergedTemplateKeys = new ArrayList<>();

		String currMergedTemplateKey = null;
		long currEpoch = -1;
		
		String rawPrefix = templatePath + indexAccessor.keyPathSeperator() + RAW_PREFIX;
		String mergedPrefix = templatePath + indexAccessor.keyPathSeperator() + MERGED_PREFIX;

		for (String templateKey : allTemplateKeys) {
			
			if (templateKey.startsWith(rawPrefix)) {
				
				rawTemplateKeys.add(templateKey);
				
			} else if (templateKey.startsWith(mergedPrefix)){
				
				if (currMergedTemplateKey == null) {

					currEpoch = templateFileEpoch(templateKey);

					if (currEpoch != -1) {
						currMergedTemplateKey = templateKey;
					}
					
				} else {
					
					long keyEpoch = templateFileEpoch(templateKey);

					if ((keyEpoch != -1) && 
						(keyEpoch > currEpoch)) {
						
						currMergedTemplateKey = templateKey;
					}
				}	
			}
		}
		
		List<String> currTemplateKeys;
		
		if (currMergedTemplateKey != null) {
			
			currTemplateKeys =  new ArrayList<>(rawTemplateKeys.size() + 1);
			
			currTemplateKeys.addAll(rawTemplateKeys);
			currTemplateKeys.add(currMergedTemplateKey);
						
		} else {
			
			currTemplateKeys = rawTemplateKeys;
		}
		
		return new IndexTemplates(indexAccessor, prefix,
			allTemplateKeys, currTemplateKeys,
			mergedTemplateKeys, rawTemplateKeys, currMergedTemplateKey);
	}
	
	public synchronized void mergeTemplateFiles() throws IOException {
							
		if (allTemplateKeys.isEmpty()) {
			return;
		}
	
		if ((rawTemplateKeys.isEmpty()) &&
			(mergedTemplateKeys.size() == 1)) {
			
			return;
		}
						
		Path mergedPath = Files.createTempFile(this.prefix + KEY_SEPERATOR + MERGED_PREFIX, null);
		File mergedFile = mergedPath.toFile();

		MessageDigest messageDigest;
		
		Set<String> templateHashes = new HashSet<>();
		
		try {
			
			messageDigest = MessageDigest.getInstance("MD5");
			
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
		

		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(mergedFile));
			
		long mergedLength = 0;

		try {
						
			for (String rawTemplateKey : this.rawTemplateKeys) {
				
				mergedLength += this.mergeTemplateFile(rawTemplateKey, 
					outputStream, templateHashes, messageDigest);
			}
			
			if (this.currMergedTemplateKey != null) {
				
				mergedLength += this.mergeTemplateFile(this.currMergedTemplateKey,
					outputStream, templateHashes, messageDigest);
			}
			
		} finally {
	
			try {
				
				outputStream.flush();
				outputStream.close();
				
			} catch (Exception e) {
					
				if (!mergedFile.delete()) {			
					logger.warn("could not delete: " + mergedFile);	
				}
				
				throw new IOException("error closing: " + mergedFile);
				
			}
		}
		
		InputStream inputStream = new BufferedInputStream(new FileInputStream(mergedFile));
		
		try {
			
			String path = indexAccessor.indexObjectPath(IndexObjectType.template,
				this.prefix);
			
			String key = templateFileKey(messageDigest, true);
			
			indexAccessor.putObject(path, key,
				inputStream, mergedLength, Collections.emptyMap());
			
			if (logger.isInfoEnabled()) {
				
				logger.info("merged templates" +
					". templates.size: " + templateHashes.size() +
					", rawTemplateFiles.size: " + rawTemplateKeys.size() +
					", currMergedTemplateFile: " + this.currMergedTemplateKey +
					", bytes: " +  mergedLength +
					", merged key: " +  key
				);
			}
		
		} finally {
		
			try {
				
				inputStream.close();
				
			} finally {
				
				if (!mergedFile.delete()) {			
					logger.warn("could not delete: " + mergedFile);	
				}			
			}
		}
		
		indexAccessor.deleteObjects(this.allTemplateKeys);
	}	
	
	private long mergeTemplateFile(String key, OutputStream outputStream,
		Set<String> templateHashes, MessageDigest messageDigest) {
		
		long result = 0;

		InputStream inputStream = null;
		File templateFile = null;
				
		try {
			
			inputStream = indexAccessor.readObject(key);

			Path templatePath = Files.createTempFile(key.replace(indexAccessor.keyPathSeperator(), "_"), null);
			templateFile = templatePath.toFile();
			
			Files.copy(inputStream, templatePath, StandardCopyOption.REPLACE_EXISTING);
			
			boolean first = true;
			
			try (BufferedReader br = new BufferedReader(new FileReader(templateFile))) {
			   
				String line;
			    
				while ((line = br.readLine()) != null) {
			      
					if (!line.startsWith(TEMPLATE_HASH_PREFIX)) {
						continue;
					}
							
					int hashEnd = line.indexOf(TEMPLATE_HASH_POSTFIX, TEMPLATE_HASH_PREFIX.length());
					
					if (hashEnd == -1) {
						continue;
					}
					
					String templateHash = line.substring(TEMPLATE_HASH_PREFIX.length(), hashEnd);
					
					if (!templateHashes.add(templateHash)) {
						continue;
					}
					
					if (first) {
						
						first = false;
						
					} else {
						
						outputStream.write(BaseIndexWriter.LINE_BREAK_BYTES);				
						result += BaseIndexWriter.LINE_BREAK_BYTES.length;
					}
					
					byte[] bytes = line.getBytes();
					
					messageDigest.update(bytes);				
					outputStream.write(bytes);
					
					result += bytes.length;
			    }
				
			}
			
		} catch (Exception e) {
			
			logger.error("error merging template file: " +  key, e);
		}
		
		finally {

			FileUtil.close(inputStream);

			if (templateFile != null) {
				templateFile.delete();
			}
		}
		
		return result;
	}
	
	public static String templateFileKey(MessageDigest messageDigest, boolean merged) {
		
		byte[] bytes = messageDigest.digest();
			
		byte[] encoded = Base64.getUrlEncoder().encode(bytes);
		
		return String.join(KEY_SEPERATOR, 
			merged ? MERGED_PREFIX : RAW_PREFIX,
			String.valueOf(System.currentTimeMillis()),
			new String(encoded)
		);
		
	}
	
	public long lastMergeEpoch() {
		
		return (this.currMergedTemplateKey != null) ?
			templateFileEpoch(this.currMergedTemplateKey) :
			-1;
	}
	
	private static long templateFileEpoch(String key) {
		
		String[] parts = key.split(KEY_SEPERATOR);
		
		if (parts.length < 2) {
			return -1;
		}
			
		String epoch = parts[1];
		
		return Long.valueOf(epoch);
	}
}
