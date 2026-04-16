package com.log10x.ext.cloud.index.query;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.eval.Evaluable;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.options.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.options.IndexQueryRequest;
import com.log10x.ext.cloud.index.util.ArgsUtil;

/**
 * A POJO for deserializing instances of the index object option group defined in:
 * {@link https://doc.log10x.com/run/input/objectStorage/query} 
 */
public class IndexQueryOptions implements IndexContainerOptions, IndexQueryRequest {
	
	private static final int DEFAULT_MAX_FUNCTION_INSTANCES = 1000;
	
	public static final int MAX_FUNCTION_INSTANCES_CAP = 10000;
	
	public final String queryName;
	
	public final String queryObjectStorageName;
	
	public final List<String> queryObjectStorageArgs;
	
	public final String queryIndexContainer;
	
	public final String queryTarget;
	
	public final String queryReadContainer;
	
	public final boolean queryReadPrintProgress;

	public final String querySearch;
	
	public final List<String> queryFilters;
	
	public final List<String> queryFilterTemplateHashes;
	
	public final List<String> queryFilterVars;
	
	public final String queryId;
	
	public final long queryElapseTime;
	
	@Evaluable
	public final long queryLimitProcessingTime;
	
	@Evaluable
	public final long queryLimitResultSize;

	@Evaluable
	public final long queryScanFunctionLimitResultSizeInterval;

	@Evaluable
	public final long queryFrom;
	
	@Evaluable
	public final long queryTo;
	
	public final String queryScanFunctionUrl;
	
	@Evaluable
	public final int queryScanFunctionParallelTimeslice;
	
	public final int queryScanFunctionParallelMaxInstances;
	
	public final int queryScanFunctionParallelThreads;
	
	public final int queryScanFlushInterval;
	
	@Evaluable
	public final int queryStreamFunctionParallelByteRange;
	
	public final int queryStreamFunctionParallelObjects;
	
	public final String queryStreamFunctionUrl;

	public final String queryLogLevels;

	public final String queryLogGroup;

	public final boolean queryWriteResults;

	public IndexQueryOptions(String queryName, String queryAccess,
		List<String> queryObjectStorageArgs, String queryIndexContainer,
		String queryReadContainer, boolean queryReadPrintProgress,
		String querySearch, List<String> queryFilters,
		List<String> queryTemplatesHashes, List<String> queryVars,
		String queryID, long queryMaxResultSize, long queryElapseTime,
		long queryMaxProcessingTime, long queryProcessingCheckInterval,
		String queryTarget, long queryFrom, long queryTo,
		int queryScanFlushInterval, String queryScanFunctionUrl,
		int queryScanFunctioTimeslice, int queryScanFunctionMaxInstances,
		int queryScanFunctionThreads, int queryStreamFunctionObjects,
		int queryStreamFunctionByteRange, String queryStreamFunctionUrl,
		String queryLogLevels, String queryLogGroup,
		boolean queryWriteResults) {

		this.queryName = queryName;
		this.queryObjectStorageName = queryAccess;
		this.queryObjectStorageArgs = queryObjectStorageArgs;
		
		this.queryTarget = queryTarget;
		this.queryFilters =  queryFilters;
		this.querySearch = querySearch;
		this.queryFrom = queryFrom;
		this.queryTo = queryTo;
		
		this.queryFilterTemplateHashes = queryTemplatesHashes;
		this.queryFilterVars = queryVars;
		
		this.queryId = queryID;
		this.queryLimitResultSize = queryMaxResultSize;
		
		this.queryLimitProcessingTime = queryMaxProcessingTime;
		this.queryScanFunctionLimitResultSizeInterval = queryProcessingCheckInterval;
		
		this.queryElapseTime = queryElapseTime;
		
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
		this.queryStreamFunctionUrl = queryStreamFunctionUrl;

		this.queryLogLevels = queryLogLevels;
		this.queryLogGroup = queryLogGroup;

		this.queryWriteResults = queryWriteResults;
	}

	protected IndexQueryOptions() {
		this(null, null, new ArrayList<>(), null, null, true, null, new LinkedList<>(),
			new LinkedList<>(), new LinkedList<>(), null, 0, 0, 0, 0, null,
			-1, -1, 0, null, 0, DEFAULT_MAX_FUNCTION_INSTANCES, 0, 0, 0, null, null, null,
			false);
	}
	
	@Override
	public List<String> queryLogLevels() {

		if (queryLogLevels == null || queryLogLevels.isBlank()) {
			return List.of();
		}
		return List.of(queryLogLevels.split(","));
	}

	@Override
	public String name() {
		return this.queryName;
	}
	
	@Override
	public String indexContainer() {
		return this.queryIndexContainer;
	}

	@Override
	public String target() {
		return this.queryTarget;
	}
	
	@Override
	public String accessorAlias() {
		return this.queryObjectStorageName;
	}

	@Override
	public String filter() {
		
		String and = ' ' + QueryFilterEvaluator.AND + ' ';
		String query = this.queryFilters.stream().collect(Collectors.joining(and));
		
		if ((this.querySearch != null) &&
			(!this.querySearch.isBlank())) {
			
			if (query.isBlank()) {
				query = this.querySearch;
			} else {
				query += and + this.querySearch;
			}
		}
		
		return query;
	}

	public boolean hasFilters() {
		return !filter().isBlank();
	}

	@Override
	public long from() {
		return this.queryFrom;
	}

	@Override
	public long to() {
		return this.queryTo;
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
	public String queryLogGroup() {
		return this.queryLogGroup;
	}

	@Override
	public String ID() {
		return this.queryId;
	}
	
	@Override
	public long elapseTime() {
		return this.queryElapseTime;
	}
	
	@Override
	public Map<String, String> args() {
		return ArgsUtil.toMap(this.queryObjectStorageArgs);
	}
}
