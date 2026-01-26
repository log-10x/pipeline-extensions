package com.log10x.ext.cloud.index.write;

import java.util.ArrayList;
import java.util.List;

/**
 * POJO mapping an input object residing within an KV storage container (e.g. AWS S3 bucket)
 * and any KV objects used to index its contents (e.g. bloom filters, metadata, etc...) . 
 */
public class TargetObjectReverseIndex {
		
	public final List<String> indexObjects;
	
	public TargetObjectReverseIndex() {
		this.indexObjects = new ArrayList<>();
	}
}
