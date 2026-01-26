package com.log10x.ext.cloud.index.write.delete;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.log10x.ext.cloud.index.interfaces.options.ObjectStorageInputContainerOptions;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * POJO defining the options for indexing a target input object read
 * from a KV storage (e.g. AWS S3). 
 * 
 * To learn more, see: {@link https://doc.log10x.com/run/input/objectStorage/index/delete}
 */
public class IndexDeleteOptions implements ObjectStorageInputContainerOptions {

	public final String indexDeleteObjectStorageName;
	
	public final List<String> indexDeleteObjectStorageArgs;

	public final String indexDeleteContainer;

	public final String indexDeleteTarget;
	
	public final String indexDeleteObject;
	
	public IndexDeleteOptions() {
		this(null, new ArrayList<>(), null, null, null);
	}
	
	public IndexDeleteOptions(String indexDeleteObjectStorageName,
		List<String> indexDeleteObjectStorageArgs,
		String indexDeleteContainer, String indexDeletePrefix,
		String indexDeleteTargetObject) {
		
		this.indexDeleteObjectStorageName = indexDeleteObjectStorageName;
		this.indexDeleteObjectStorageArgs = indexDeleteObjectStorageArgs;
		this.indexDeleteContainer = indexDeleteContainer;
		this.indexDeleteTarget = indexDeletePrefix;
		this.indexDeleteObject = indexDeleteTargetObject;	
	}
	
	@Override
	public Map<String, String> args() {
		return ArgsUtil.toMap(indexDeleteObjectStorageArgs);
	}

	@Override
	public String accessorAlias() {
		return this.indexDeleteObjectStorageName;
	}

	@Override
	public String indexContainer() {
		return this.indexDeleteContainer;
	}

	@Override
	public String inputContainer() {
		return this.indexDeleteContainer;
	}

	@Override
	public String inputObject() {
		return this.indexDeleteObject;
	}

	@Override
	public String target() {
		return this.indexDeleteTarget;
	}	
}
