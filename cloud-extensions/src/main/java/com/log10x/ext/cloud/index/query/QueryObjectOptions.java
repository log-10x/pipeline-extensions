package com.log10x.ext.cloud.index.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.log10x.ext.cloud.index.interfaces.QueryObjectRequest;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * Defines a serializable request POJO to fetch a set of byte ranges
 * from an input blob residing within a cloud storage, and extract events
 * falling within a time range and matching search terms.
 * To learn more more, see:
 * {@link http://doc.log10x.com/run/input/objectStorage/query/object }
 */
public class QueryObjectOptions implements QueryObjectRequest {
	
	public final String objectStorageName;
	
	public final List<String> objectStorageArgs;

	public final List<String> terms;

	public final String target;
	
	public final String container;

	public final long from;

	public final long to;
	
	public final long[] byteRanges;	
		
	private transient int currByteRangeIndex;
	
	public QueryObjectOptions(
		String objectStorageName, List<String> objectStorageArgs,
		List<String> terms, 
		String target, String container,
		long from, long to, long[] byteRanges) {
		
		this.objectStorageName = objectStorageName;
		this.objectStorageArgs = objectStorageArgs;
		
		this.terms = terms;
		this.target = target;
		this.container = container;
		this.from = from;
		this.to = to;
		this.byteRanges = byteRanges;
	}
	
	public QueryObjectOptions(QueryObjectRequest other, int segmentSize) {
		
		this(other.accessorAlias(), ArgsUtil.toList(other.args()),
			other.terms(), other.inputObject(), other.inputContainer(),
			other.from(), other.to(), new long [segmentSize * 2]);
	}

	public QueryObjectOptions() {
		this(null, new ArrayList<>(), new ArrayList<>(), null, null, 0, 0, null);
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
	public String inputObject() {
		return this.target;
	}

	@Override
	public String accessorAlias() {
		return this.objectStorageName;
	}

	@Override
	public List<String> terms() {
		return this.terms;
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
		return null;
	}
	
	@Override
	public Map<String, String> args() {
		
		return ArgsUtil.toMap(objectStorageArgs);
	}
}
