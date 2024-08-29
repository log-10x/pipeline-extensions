package com.log10x.ext.cloud.index.query;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.IndexQueryRequest;
import com.log10x.ext.cloud.index.util.ArgsUtil;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * A POJO for deserializing instances of the index object option group defined in:
 * {@link http://doc.log10x.com/run/input/objectStorage/query} 
 */
public class IndexQueryOptions implements IndexContainerOptions, IndexQueryRequest {
	
	private static final int DEFAULT_MAX_FUNCTION_INSTANCES = 1000;
	
	public static final int MAX_FUNCTION_INSTANCES_CAP = 10000;
	
	public final String queryObjectStorageName;
	
	public final List<String> queryObjectStorageArgs;
	
	public final String queryIndexContainer;
	
	public final String queryFilterPrefix;
	
	public final String queryReadContainer;
	
	public final boolean queryReadPrintProgress;

	public final List<String> queryFilterTerms;
	
	public final List<String> queryFilterTemplateHashes;
	
	public final List<String> queryFilterVars;

	public final long queryFilterFrom;
	
	public final long queryFilterTo;
	
	public final String queryScanFunctionUrl;
	
	public final String queryScanFunctionParallelTimeslice;
	
	public final int queryScanFunctionParallelMaxInstances;
	
	public final int queryScanFunctionParallelThreads;
	
	public final String queryScanFlushInterval;
	
	public final String queryStreamFunctionParallelByteRange;
	
	public final int queryStreamFunctionParallelObjects;
	
	public final String queryActions;
	
	public final String queryStreamFunctionUrl;

	public IndexQueryOptions(String queryAccess, List<String> queryObjectStorageArgs,
		String queryIndexContainer, String queryReadContainer, boolean queryReadPrintProgress,
		List<String> queryFilterTerms, List<String> queryTemplatesHashes, List<String> queryVars,
		String queryFilterPrefix, long queryFilterFrom, long queryFilterTo,
		String queryScanFlushInterval, String queryScanFunctionUrl, 
		String queryScanFunctioTimeslice, int queryScanFunctionMaxInstances,
		int queryScanFunctionThreads, int queryStreamFunctionObjects,
		String queryStreamFunctionByteRange, String queryActions,
		String queryStreamFunctionUrl) {
		
		this.queryObjectStorageName = queryAccess;
		this.queryObjectStorageArgs = queryObjectStorageArgs;
		
		this.queryFilterPrefix = queryFilterPrefix;
		this.queryFilterTerms =  queryFilterTerms;
		this.queryFilterFrom = queryFilterFrom;
		this.queryFilterTo = queryFilterTo;
		
		this.queryFilterTemplateHashes = queryTemplatesHashes;
		this.queryFilterVars = queryVars;
		
		this.queryIndexContainer = queryIndexContainer;
		this.queryScanFunctionParallelTimeslice = queryScanFunctioTimeslice;
		this.queryScanFunctionParallelMaxInstances = queryScanFunctionMaxInstances;
		this.queryScanFunctionParallelThreads = queryScanFunctionThreads;
		this.queryScanFlushInterval = queryScanFlushInterval;
		this.queryScanFunctionUrl = queryScanFunctionUrl;
		
		this.queryReadContainer = queryReadContainer;
		this.queryReadPrintProgress = queryReadPrintProgress;
		this.queryStreamFunctionParallelObjects = queryStreamFunctionObjects;
		this.queryStreamFunctionParallelByteRange = queryStreamFunctionByteRange;
		this.queryActions = queryActions;
		this.queryStreamFunctionUrl = queryStreamFunctionUrl;
	}
	
	public IndexQueryOptions(IndexQueryOptions other) {
		
		this(other.queryObjectStorageName, other.queryObjectStorageArgs,
			 other.queryIndexContainer, 
			 other.queryReadContainer, other.queryReadPrintProgress, other.queryFilterTerms,
			 other.queryFilterTemplateHashes, other.queryFilterVars,
			 other.queryFilterPrefix, other.queryFilterFrom, other.queryFilterTo,
			 other.queryScanFlushInterval, other.queryScanFunctionUrl,
			 other.queryScanFunctionParallelTimeslice, other.queryScanFunctionParallelMaxInstances,
			 other.queryScanFunctionParallelThreads, other.queryStreamFunctionParallelObjects,
			 other.queryStreamFunctionParallelByteRange,
			 other.queryActions, other.queryStreamFunctionUrl);			
	}
	
	protected IndexQueryOptions() {
		this(null, new ArrayList<>(), null, null, true, new LinkedList<>(),
			new LinkedList<>(), new LinkedList<>(), null,
			-1, -1, null, null, null, DEFAULT_MAX_FUNCTION_INSTANCES, 0, 0, null, null, null);
	}
	
	@Override
	public String indexContainer() {
		return this.queryIndexContainer;
	}

	@Override
	public String prefix() {
		return this.queryFilterPrefix;
	}
	
	@Override
	public String accessorAlias() {
		return this.queryObjectStorageName;
	}

	@Override
	public List<String> terms() {
		return this.queryFilterTerms;
	}

	@Override
	public long from() {
		return this.queryFilterFrom;
	}

	@Override
	public long to() {
		return this.queryFilterTo;
	}
	
	@Override
	public String toString() {
	
		try {
			return MapperUtil.jsonMapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return e.toString();
		}
	}

	@Override
	public String inputContainer() {
		return null;
	}
	
	@Override
	public Map<String, String> args() {
		
		return ArgsUtil.toMap(queryObjectStorageArgs);
	}
}
