package com.log10x.ext.cloud.index.write;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageInputContainerOptions;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * POJO defining the options for indexing a target input object read
 * from a KV storage (e.g. AWS S3). 
 * 
 * To learn more, see: {@link http://doc.log10x.com/run/input/objectStorage/index/#options}
 */
public class IndexWriteOptions implements IndexContainerOptions, ObjectStorageInputContainerOptions {
	
	public final String indexObjectStorageName;
	
	public final List<String> indexObjectStorageArgs;

	public final String indexWriteContainer;

	public final String indexWritePrefix;

	public final String indexWriteByteRange;

	public final String indexWriteResolution;
	
	public final int indexWriteAccuracy;

	public final String indexReadContainer;

	public final String indexReadObject;
	
	public final boolean indexReadPrintProgress;
		
	private transient String key;

	public IndexWriteOptions(
		String indexObjectStorageName,List<String> indexObjectStorageArgs,
		String indexWriteContainer, 
		String indexWritePrefix, 
		String indexReadObject, String indexReadContainer,
		boolean indexReadPrintProgress, String indexWriteByteRange,
		String indexWriteResolution, int indexWriteAccuracy) {
			
		this.indexObjectStorageName = indexObjectStorageName;
		this.indexObjectStorageArgs = indexObjectStorageArgs;

		this.indexWriteContainer = indexWriteContainer;
		this.indexWritePrefix = indexWritePrefix;
		
		this.indexReadObject = indexReadObject;
		this.indexReadContainer = indexReadContainer;
		this.indexReadPrintProgress = indexReadPrintProgress;
		
		this.indexWriteByteRange = indexWriteByteRange; 
		this.indexWriteResolution = indexWriteResolution;
		this.indexWriteAccuracy = indexWriteAccuracy;
	}
	
	public IndexWriteOptions() {
		this(null, new ArrayList<>(),  null, null, null, null, true, null, null, 0);	
	}
	
	public int indexFetchErrorProb() {
		return 100 - this.indexWriteAccuracy;
	}
	
	@Override
	public String indexContainer() {
		return this.indexWriteContainer;
	}

	@Override
	public String prefix() {
		return this.indexWritePrefix;
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
