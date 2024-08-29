package com.log10x.ext.cloud.index.object;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.QueryObjectRequest;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * A POJO for deserializing instances of the index query option group defined in:
 * {@link http://doc.log10x.com/run/input/objectStorage/query/object } 
 */
public class IndexQueryObjectOptions implements QueryObjectRequest, IndexContainerOptions {
	
	public final String queryObjectObjectStorageName;
	
	public final List<String> queryObjectObjectStorageArgs;

	public final List<String> queryObjectTerms;

	public final String queryObjectTarget;
	
	public final String queryObjectContainer;
	
	public final boolean queryObjectPrintProgress;

	public final long queryObjectFrom;

	public final long queryObjectTo;
	
	public final long[] queryObjectByteRanges;	
		
	private transient int currByteRangeIndex;
	
	public IndexQueryObjectOptions(
		String queryObjectObjectStorageName, List<String> queryObjectObjectStorageArgs,
		String queryObjectContainer,
		List<String> queryObjectTerms, String queryObjectTarget,
		boolean queryObjectPrintProgress, long queryObjectFrom,
		long queryObjectTo, long[] queryObjectByteRanges) {
		
		this.queryObjectObjectStorageName = queryObjectObjectStorageName;
		this.queryObjectObjectStorageArgs = queryObjectObjectStorageArgs;
		this.queryObjectContainer = queryObjectContainer;
		this.queryObjectTerms = queryObjectTerms;
		this.queryObjectTarget = queryObjectTarget;
		this.queryObjectPrintProgress = queryObjectPrintProgress;
		this.queryObjectFrom = queryObjectFrom;
		this.queryObjectTo = queryObjectTo;
		this.queryObjectByteRanges = queryObjectByteRanges;
	}
	
	public IndexQueryObjectOptions() {
		this(null, new ArrayList<>(), null, new ArrayList<>(), null, true, 0, 0, null);
	}

	@Override
	public int size() {
		return queryObjectByteRanges.length / 2;
	}

	@Override
	public long offset(int index) {
		return this.queryObjectByteRanges[index * 2];
	}

	@Override
	public int length(int index) {
		return (int)this.queryObjectByteRanges[(index * 2) + 1];
	}
	
	@Override
	public String accessorAlias() {
		return this.queryObjectObjectStorageName;
	}

	@Override
	public void add(long off, int len) {
		
		this.queryObjectByteRanges[(this.currByteRangeIndex * 2)] = off;
		this.queryObjectByteRanges[(this.currByteRangeIndex * 2) + 1] =  len;
		
		this.currByteRangeIndex++;
	}

	@Override
	public String inputContainer() {
		return this.queryObjectContainer;
	}

	@Override
	public String inputObject() {
		return this.queryObjectTarget;
	}

	@Override
	public List<String> terms() {
		return this.queryObjectTerms;
	}

	@Override
	public long from() {
		return this.queryObjectFrom;
	}

	@Override
	public long to() {
		return this.queryObjectTo;
	}

	@Override
	public long[] byteRanges() {
		return this.queryObjectByteRanges;
	}

	@Override
	public String indexContainer() {
		return null;
	}

	@Override
	public String prefix() {
		return null;
	}

	@Override
	public Map<String, String> args() {
		
		return ArgsUtil.toMap(this.queryObjectObjectStorageArgs);
	}
}
