package com.log10x.ext.cloud.index.write;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.log10x.ext.cloud.index.interfaces.options.ObjectStorageInputContainerOptions;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * POJO defining the options for indexing a target input object read
 * from a KV storage (e.g. AWS S3). 
 * 
 * To learn more, see: {@link https://doc.log10x.com/run/input/objectStorage/index/#options}
 */
public class IndexWriteOptions implements ObjectStorageInputContainerOptions {
	
	public final String indexObjectStorageName;
	
	public final List<String> indexObjectStorageArgs;

	public final String indexWriteContainer;

	public final String indexWriteTarget;

	public final int indexWriteByteRange;

	public final int indexWriteResolution;
	
	public final int indexWriteAccuracy;

	public final long indexWriteTemplateMergeInterval;

	public final String indexReadObject;

	public final String indexReadContainer;

	public final String indexReadMessageField;

	public final boolean indexReadPrintProgress;

	private transient String key;

	public IndexWriteOptions(
		String indexObjectStorageName,List<String> indexObjectStorageArgs,
		String indexWriteContainer, String indexWritePrefix, 
		String indexReadObject, String indexReadContainer,
		String indexReadMessageField, boolean indexReadPrintProgress,
		int indexWriteByteRange, int indexWriteResolution,
		int indexWriteAccuracy, long indexWriteTemplateMergeInterval) {
			
		this.indexObjectStorageName = indexObjectStorageName;
		this.indexObjectStorageArgs = indexObjectStorageArgs;

		this.indexWriteContainer = indexWriteContainer;
		this.indexWriteTarget = indexWritePrefix;
		
		this.indexReadObject = indexReadObject;
		this.indexReadContainer = indexReadContainer;
		this.indexReadMessageField = indexReadMessageField;
		this.indexReadPrintProgress = indexReadPrintProgress;
		
		this.indexWriteByteRange = indexWriteByteRange; 
		this.indexWriteResolution = indexWriteResolution;
		this.indexWriteAccuracy = indexWriteAccuracy;
		
		this.indexWriteTemplateMergeInterval = indexWriteTemplateMergeInterval;
	}
	
	public IndexWriteOptions() {
		
		this(null, new ArrayList<>(), null, null, null, null, null, true, 0, 0, 0, 0);	
	}
	
	public int indexFetchErrorProb() {
		return 100 - this.indexWriteAccuracy;
	}
	
	@Override
	public String indexContainer() {
		return this.indexWriteContainer;
	}

	@Override
	public String target() {
		return this.indexWriteTarget;
	}

	@Override
	public String accessorAlias() {
		return this.indexObjectStorageName;
	}

	@Override
	public String inputContainer() {
		return this.indexReadContainer;
	}

	@Override
	public String inputObject() {
		return this.indexReadObject;
	}
	
	public String key() {
		
		if (this.key != null) {
			return this.key;
		}
		
		this.key = String.join("_", 
			this.indexObjectStorageName,
			this.indexReadContainer,
			this.indexReadObject,
			String.valueOf(this.indexWriteByteRange));
	
		return this.key;
	}
	
	@Override
	public Map<String, String> args() {
		
		return ArgsUtil.toMap(indexObjectStorageArgs);
	}
}
