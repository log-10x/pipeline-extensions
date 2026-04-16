package com.log10x.ext.cloud.index.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.options.QueryObjectRequest;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * Defines a serializable request POJO to fetch a set of byte ranges
 * from an input blob residing within a cloud storage, and extract events
 * falling within a time range and matching search terms.
 * To learn more more, see:
 * {@link https://doc.log10x.com/run/input/objectStorage/query/object }
 */
public class QueryObjectOptions implements QueryObjectRequest {
	
	public final String queryName;
	
	public final String objectStorageName;
	
	public final List<String> objectStorageArgs;

	public final String filter;

	public final String target;
	
	public final String targetObject;
	
	public final String container;
	
	public final String indexContainer;

	public final long from;

	public final long to;
	
	public final long[] byteRanges;	
	
	public final String ID;

	public final long elapseTime;

	public final List<String> logLevels;

	public final String logGroup;

	public final boolean writeResults;

	private transient int currByteRangeIndex;

	public QueryObjectOptions(String queryName, String objectStorageName, List<String> objectStorageArgs,
			String filter, String target, String targetObject, String container, String indexContainer,
			long from, long to, long[] byteRanges, String ID, long elapseTime,
			List<String> logLevels, String logGroup, boolean writeResults) {

		this.queryName = queryName;
		this.objectStorageName = objectStorageName;
		this.objectStorageArgs = objectStorageArgs;
		this.filter = filter;

		this.target = target;
		this.targetObject = targetObject;
		this.container = container;
		this.indexContainer = indexContainer;

		this.from = from;
		this.to = to;
		this.byteRanges = byteRanges;

		this.ID = ID;
		this.elapseTime = elapseTime;
		this.logLevels = logLevels;
		this.logGroup = logGroup;

		this.writeResults = writeResults;
	}

	public QueryObjectOptions(QueryObjectRequest other, int segmentSize) {

		this(other.name(), other.accessorAlias(), ArgsUtil.toList(other.args()),
			other.filter(), other.target(), other.inputObject(),
			other.inputContainer(), other.indexContainer(),
			other.from(), other.to(), new long [segmentSize * 2],
			other.ID(), other.elapseTime(), other.queryLogLevels(), other.queryLogGroup(),
			false);
	}

	public QueryObjectOptions() {
		this(null, null, new ArrayList<>(), null, null, null, null, null, 0, 0, null, null, 0, null, null, false);
	}
	
	@Override
	public String name() {
		return this.queryName;
	}
	
	@Override
	public int size() {
		return byteRanges.length / 2;
	}

	@Override
	public long offset(int index) {
		return this.byteRanges[(index * 2)];
	}

	@Override
	public int length(int index) {
		return (int) this.byteRanges[(index * 2) + 1];
	}

	@Override
	public void add(long off, int len) {

		this.byteRanges[(this.currByteRangeIndex * 2)] = off;
		this.byteRanges[(this.currByteRangeIndex * 2) + 1] = len;

		this.currByteRangeIndex++;
	}

	@Override
	public String inputContainer() {
		return this.container;
	}

	@Override
	public String target() {
		return this.target;
	}
	
	@Override
	public String inputObject() {
		return this.targetObject;
	}

	@Override
	public String accessorAlias() {
		return this.objectStorageName;
	}

	@Override
	public String filter() {
		return this.filter;
	}

	@Override
	public long from() {
		return this.from;
	}

	@Override
	public long to() {
		return this.to;
	}

	@Override
	public long[] byteRanges() {
		return this.byteRanges;
	}

	public QueryObjectOptions subOptions(int from, int to) {

		int size = to - from;
		
		QueryObjectOptions result = new QueryObjectOptions(this, size);
		
		System.arraycopy(
				this.byteRanges(), from * 2,
				result.byteRanges(), 0,
				size * 2);
		
		return result;
	}

	@Override
	public String indexContainer() {
		return this.indexContainer;
	}
	
	@Override
	public String ID() {
		return this.ID;
	}
	
	@Override
	public long elapseTime() {
		return this.elapseTime;
	}
	
	@Override
	public List<String> queryLogLevels() {
		return this.logLevels;
	}
	
	@Override
	public String queryLogGroup() {
		return this.logGroup;
	}

	@Override
	public Map<String, String> args() {

		return ArgsUtil.toMap(objectStorageArgs);
	}

	@Override
	public String toString() {
	
		try {
			return MapperUtil.jsonMapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return e.toString();
		}
	}
}
