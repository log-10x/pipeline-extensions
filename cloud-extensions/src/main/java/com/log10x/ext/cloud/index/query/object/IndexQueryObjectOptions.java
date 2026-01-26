package com.log10x.ext.cloud.index.query.object;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.log10x.ext.cloud.index.interfaces.options.QueryObjectRequest;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * A POJO for deserializing instances of the index query option group defined in:
 * {@link https://doc.log10x.com/run/input/objectStorage/query/object } 
 */
public class IndexQueryObjectOptions implements QueryObjectRequest {
	
	public final String queryObjectQueryName;
	
	public final String queryObjectObjectStorageName;
	
	public final List<String> queryObjectObjectStorageArgs;

	public final String queryObjectFilter;

	public final String queryObjectTarget;
	
	public final String queryObjectTargetObject;
	
	public final String queryObjectContainer;
	
	public final String queryObjectIndexContainer;

	public final boolean queryObjectPrintProgress;

	public final long queryObjectFrom;

	public final long queryObjectTo;
	
	public final long[] queryObjectByteRanges;	
		
	public final String queryObjectID;	
	
	public final long queryObjectElapseTime;

	public final List<String> queryObjectLogLevels;

	public final String queryObjectLogGroup;

	private transient int currByteRangeIndex;

	public IndexQueryObjectOptions(String queryObjectQueryName, String queryObjectObjectStorageName,
			List<String> queryObjectObjectStorageArgs, String queryObjectContainer, String queryObjectIndexContainer,
			String queryObjectFilter, String queryObjectTarget, String queryObjectTargetObject,
			boolean queryObjectPrintProgress, long queryObjectFrom, long queryObjectTo, long[] queryObjectByteRanges,
			String queryObjectID, long queryObjectElapseTime, List<String> queryObjectLogLevels,
			String queryObjectLogGroup) {

		this.queryObjectQueryName = queryObjectQueryName;
		this.queryObjectObjectStorageName = queryObjectObjectStorageName;
		this.queryObjectObjectStorageArgs = queryObjectObjectStorageArgs;

		this.queryObjectContainer = queryObjectContainer;
		this.queryObjectIndexContainer = queryObjectIndexContainer;

		this.queryObjectFilter = queryObjectFilter;
		this.queryObjectTarget = queryObjectTarget;
		this.queryObjectTargetObject = queryObjectTargetObject;
		this.queryObjectPrintProgress = queryObjectPrintProgress;
		this.queryObjectFrom = queryObjectFrom;
		this.queryObjectTo = queryObjectTo;
		this.queryObjectByteRanges = queryObjectByteRanges;
		this.queryObjectID = queryObjectID;
		this.queryObjectElapseTime = queryObjectElapseTime;
		this.queryObjectLogLevels = queryObjectLogLevels;
		this.queryObjectLogGroup = queryObjectLogGroup;
	}

	public IndexQueryObjectOptions() {

		this(null, null, new ArrayList<>(), null, null, null,
			null, null, true, 0, 0, null, null, 0, null, null);
	}

	@Override
	public String name() {
		return this.queryObjectQueryName;
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
		return this.queryObjectTargetObject;
	}

	@Override
	public String filter() {
		return this.queryObjectFilter;
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
		return this.queryObjectIndexContainer;
	}

	@Override
	public String target() {
		return this.queryObjectTarget;
	}
	
	@Override
	public String ID() {
		return this.queryObjectID;
	}
	
	@Override
	public long elapseTime() {
		return this.queryObjectElapseTime;
	}

	@Override
	public List<String> queryLogLevels() {
		return this.queryObjectLogLevels;
	}
	
	@Override
	public String queryLogGroup() {
		return this.queryObjectLogGroup;
	}

	@Override
	public Map<String, String> args() {

		return ArgsUtil.toMap(this.queryObjectObjectStorageArgs);
	}
}
