package com.log10x.ext.cloud.index.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.log10x.ext.cloud.index.BaseIndexWriter;
import com.log10x.ext.cloud.index.client.IndexQueryClient;
import com.log10x.ext.cloud.index.filter.DecodedBloomFilter;
import com.log10x.ext.cloud.index.interfaces.IndexObjectKey;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.util.ExecutorUtil;
import com.log10x.ext.cloud.index.write.IndexWriteTarget;
import com.log10x.ext.cloud.index.write.IndexWriteTarget.ByteRange;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.invoke.PipelineLaunchRequest;
import com.log10x.ext.edge.json.MapperUtil;

import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;

/**
 * This class is used to produce a set of requests to a remote end-point 
 * to fetch a target set of input objects byte ranges residing
 * within a KV storage and scan them for events falling within 
 * a specific time range and matching a set of search terms.
 * To learn more about how this class is instantiated by a host l1x pipeline, see: 
 * {@link https://github.com/l1x-co/config/blob/main/pipelines/run/modules/input/index/query/stream.yaml }
 */
public class IndexQueryWriter extends BaseIndexWriter {
	
	protected static final YAMLMapper yamlMapper = MapperUtil.initMapper(YAMLMapper.builder());

	private static final Logger logger = LogManager.getLogger(IndexQueryWriter.class);
	
	private static final String STREAM_SUFFIX = "-stream";
	
	private static PipelineLaunchRequest streamRequest(String name) {
		
		return PipelineLaunchRequest.newBuilder()
				.withLaunchArg(PipelineLaunchRequest.RUNTIME_NAME_ARG, name)
				.withIncludes("modules/input/objectStorage/query/object", "modules/input/objectStorage")
				.build();
	}
	
	private static final String SUB_QUERY_SUFFIX = "-subq";
	
	private static PipelineLaunchRequest queryRequest(String name) {
		
		return PipelineLaunchRequest.newBuilder()
				.withLaunchArg(PipelineLaunchRequest.RUNTIME_NAME_ARG, name)
				.withIncludes("modules/input/objectStorage/query", "modules/input/objectStorage")
				.build();
	}

	protected static class ByteRangeComparator implements Comparator<ByteRange> {
		
		protected static final ByteRangeComparator INSTANCE = new ByteRangeComparator();
		
		@Override
		public int compare(ByteRange o1, ByteRange o2) {
			
			long off1 = o1.offset;
			long off2 = o2.offset;

			if (off1 != off2) {
				return Long.compare(off1, off2);
			}
			
			int length1 = o1.length;
			int length2 = o2.length;
			
			return Integer.compare(length1, length2);
		}
	}

	protected static class IndexQueryEvent {

		public String templateHash;
		
		public Object[] var;
		
		@Override
		public String toString() {
			
			return 
				"templateHash: " + templateHash + 
				" var: " + ((var != null) ? Arrays.asList(var) : null);
		}
	}
		
	protected class IterateIndexObjectsTask implements Runnable {

		protected final long toEpoch;
		
		protected final Collection<String> indexObjectKeys;
		
		protected IterateIndexObjectsTask(long toEpoch, Collection<String> indexObjectKeys) {
			this.toEpoch = toEpoch;
			this.indexObjectKeys = indexObjectKeys;
		}
		
		@Override
		public void run() {
			
			String threadName = Thread.currentThread().getName();
			
			try {
				
				Thread.currentThread().setName(this.getClass().getSimpleName() + 
					" toEpoch: " + this.toEpoch + " " +
					" -> " + indexObjectKeys.size());
				
				iterateIndexObjects(this.toEpoch, this.indexObjectKeys);	
				
			} catch (Exception e) {
				
				logger.error("error iterating index objects: " + this, e);
				
			} finally {
				
				Thread.currentThread().setName(threadName);
			}
		}
		
		@Override
		public String toString() {
			
			return String.format("toEpoch: from: %d indexObjectKeys: %d", 
				toEpoch, indexObjectKeys.size());
		}
	}

	protected class ScanIndexRangeTask implements Runnable {

		protected final long fromEpoch;
		
		protected final long toEpoch;
		
		protected final int interval;

		protected ScanIndexRangeTask(long fromEpoch, long toEpoch, int interval) {
			this.fromEpoch = fromEpoch;
			this.toEpoch = toEpoch;
			this.interval = interval;
		}
		
		@Override
		public void run() {
			
			String threadName = Thread.currentThread().getName();
			
			try {
				
				Thread.currentThread().setName(this.getClass().getSimpleName() + 
					" interval: " + this.interval + " " +
					this.fromEpoch  + " -> " + this.toEpoch);
				
				scanIndexRange(this.fromEpoch, this.toEpoch);
				
				latch.countDown();
				
			} catch (Exception e) {
				
				logger.error("error scanning index range: " + this, e);
				
			} finally {
				
				Thread.currentThread().setName(threadName);
			}
		}	
		
		@Override
		public String toString() {
			
			return String.format("interval %d from: %d to: %d", 
				interval, fromEpoch, toEpoch);
		}
	}
	
	private final IndexQueryEvent currInputValue;
	
	private final Set<String> templateHashes;
	
	private final Set<String> vars;
	
	private final IndexQueryClient scanFunctionClient;
	
	private final IndexQueryClient streamFunctionClient;
	
	private final PipelineLaunchRequest launchRequest;
	
	private final long timeslice;
	
	private final String basePipelineName;
	
	private final long flushInterval;
	
	private final long byteRange;

	private final LongObjectMap<IndexWriteTarget> writeTargets;
	private final Set<String> submittedKeyHashes;
	private final Object keyHashesLock;
	private final AtomicInteger subStreams;
	
	private ExecutorService executorService;
	private CountDownLatch latch;
	
	private volatile boolean closed;

	/**
	 * this constructor is invoked by the l1x run-time.
	 * 
	 * @param 	args
	 * 			a map of arguments passed to the l1x cli for the
	 *  		target output for which this stream is instantiated
	 * 			containing user configuration of this query
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an l1x evaluator bean which allows this output
	 * 			to interact with the l1x run-time
	 */
	public IndexQueryWriter(Map<String, Object> args, EvaluatorBean evaluatorBean) 
		throws JsonMappingException, JsonProcessingException, IllegalArgumentException{
			
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexQueryOptions.class), null, evaluatorBean);
	}

	public IndexQueryWriter(IndexQueryOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) 
			throws JsonMappingException, JsonProcessingException {

		super(options, indexAccessor, evaluatorBean);
		
		validateOptions(options);
		
		this.currInputValue = new IndexQueryEvent();
		
		this.templateHashes = new HashSet<String>(options.queryFilterTemplateHashes);
		this.vars = new HashSet<String>(options.queryFilterVars);
		
		this.scanFunctionClient = new IndexQueryClient(this.indexAccessor, options.queryScanFunctionUrl);	
		
		this.streamFunctionClient = options.queryScanFunctionUrl.equals(options.queryStreamFunctionUrl) ?
			this.scanFunctionClient :
			new IndexQueryClient(this.indexAccessor, options.queryStreamFunctionUrl);
	
		this.flushInterval = this.parse("parseDuration", options.queryScanFlushInterval);
		this.byteRange = this.parse("parseBytes", options.queryStreamFunctionParallelByteRange);
		
		this.launchRequest = launchRequest(options.queryActions);
		
		this.timeslice = calcTimeSlice(options);
		
		this.basePipelineName = (String)this.evaluator().env(PipelineLaunchRequest.RUNTIME_NAME_ARG);
		
		this.writeTargets = new LongObjectHashMap<>();
		this.submittedKeyHashes = new HashSet<>();
		this.keyHashesLock = new Object();
		this.subStreams = new AtomicInteger();
	}
	
	private static void validateOptions(IndexQueryOptions options) {

		long timerange = options.queryFilterTo - options.queryFilterFrom;
		
		if (timerange <= 0) {
			
			throw new IllegalArgumentException("illegal timerange, from: " + 
				options.queryFilterFrom + ", to: " + options.queryFilterTo);
		}
		
		int maxFunctionInstances = options.queryScanFunctionParallelMaxInstances;

		if (maxFunctionInstances <= 0) {

			throw new IllegalArgumentException("max instances must be positive. value:" + maxFunctionInstances);

		} else if (maxFunctionInstances > IndexQueryOptions.MAX_FUNCTION_INSTANCES_CAP) {

			throw new IllegalArgumentException("max instances can't be over "
					+ IndexQueryOptions.MAX_FUNCTION_INSTANCES_CAP + ". value:" + maxFunctionInstances);
		}
		
		int threadCount = options.queryScanFunctionParallelThreads;
		
		if (threadCount < 0) {
			throw new IllegalArgumentException("parallel threads can't be negative. value:" + threadCount);
		}
	}

	private long calcTimeSlice(IndexQueryOptions options) {
		
		long baseTimeSlice = this.parse("parseDuration", options.queryScanFunctionParallelTimeslice);
		
		if (baseTimeSlice <= 0) {
			return 0;
		}
		
		int maxFunctionInstances = options.queryScanFunctionParallelMaxInstances;
		
		long timerange = options.queryFilterTo - options.queryFilterFrom;
		
		if (baseTimeSlice * maxFunctionInstances >= timerange) {
			return baseTimeSlice;
		}
		
		int timeSliceMultiplier = (int)Math.ceil(timerange / (double)(baseTimeSlice * maxFunctionInstances));
		
		return timeSliceMultiplier * baseTimeSlice;
	}
	
	private static PipelineLaunchRequest launchRequest(String pipelineArgs) 
		throws JsonMappingException, JsonProcessingException {
		
		if ((pipelineArgs == null) || 
			(pipelineArgs.isBlank())) {
			
			return null;
		}
		
		String value = pipelineArgs.strip();
		
		ObjectMapper mapper = ((value.startsWith("{")) || (value.startsWith("["))) ?
			MapperUtil.jsonMapper : 
			yamlMapper;
		
		return mapper.readValue(value, PipelineLaunchRequest.class);
	}
	
	@Override
	public synchronized void flush() throws IOException {
		
		if (currChars.builder.isEmpty()) {
			return;
		}
		
		try {
			
			MapperUtil.jsonMapper.
				readerForUpdating(this.currInputValue).
				readValue(this.currChars);
	
			if (currInputValue.templateHash != null) {			
				
				templateHashes.add(currInputValue.templateHash);
				
			} else if (currInputValue.var != null) {
				
				for (Object var : currInputValue.var) {				
					vars.add(String.valueOf(var));
				}		
			}
			
		} finally {
			
			super.flush();
		}
	}
		
	private void iterateIndexObjects(long toEpoch,
		Collection<String> indexObjectKeys) throws JsonProcessingException {
		
		Set<String> localLubmittedKeyHashes = new HashSet<>();
		Map<String, Set<ByteRange>> output = new LinkedHashMap<>();

		String prefix = options.prefix();
		
		long lastSubmit = System.currentTimeMillis();
		long lastEpoch = -1;
		
		for (String indexObjectKey : indexObjectKeys) {
		
			try {
				
				IndexObjectKey key = IndexObjectKey.fromPath(indexAccessor, prefix, indexObjectKey, true);
				
				long epochValue = Long.valueOf(key.epoch);
				
				if (epochValue >= toEpoch) {		
					break;
				}
				
				String submitHash = String.valueOf(key.targetHash) + "_" + String.valueOf(key.byteRangeIndex);
				
				if (localLubmittedKeyHashes.contains(submitHash)) {
					continue;
				}
				
				// We only want to submit once we scanned all of objects for a
				// given epoch value, to prevent a case where multiple 'stream'
				// functions stream the same timestamp.
				//
				if ((this.flushInterval > 0) &&
					(epochValue != lastEpoch)) {
					
					long now = System.currentTimeMillis();
					
					if (now - lastSubmit > this.flushInterval) {
						
						this.submitMatchingByteRanges(output);
						output.clear();
						
						lastSubmit = System.currentTimeMillis();
					}
				}
				
				lastEpoch = epochValue;
				
				String encodedFilter = key.encodedFilter;
				
				DecodedBloomFilter filter = new DecodedBloomFilter(encodedFilter);
				
				if (!filter.testAll(this.vars)) {
					continue;
				}
				
				if (!filter.testAny(this.templateHashes)) {
					continue;
				}
				
				IndexWriteTarget indexWriteTarget;
				
				localLubmittedKeyHashes.add(submitHash);
				
				synchronized (this.keyHashesLock) {
					if (!this.submittedKeyHashes.add(submitHash)) {
						continue;
					}
					
					indexWriteTarget = getWriteTarget(key);
				}
				
				if (indexWriteTarget == null) {
					throw new IllegalStateException("Missing write target - " + key);
				}
				
				Set<ByteRange> indexObjects = output.get(indexWriteTarget.target);
				
				if (indexObjects == null) {
					
					indexObjects = new HashSet<>();
					output.put(indexWriteTarget.target, indexObjects);
				}
				
				ByteRange byteRange = indexWriteTarget.byteRanges.get(key.byteRangeIndex);
				
				indexObjects.add(byteRange);
			
			} catch (Exception e) {
				
				logger.error("error testing: " + indexObjectKey, e);
				continue;
			}

		}
		
		this.submitMatchingByteRanges(output);
	}
	
	private IndexWriteTarget getWriteTarget(IndexObjectKey key) throws IOException {
		
		IndexWriteTarget result = this.writeTargets.get(key.targetHash);
		
		if (result == null) {
			String metaIndexPath = this.indexAccessor.metaIndexPath(options.prefix());
			String indexName = String.valueOf(key.targetHash);
			
			String path = metaIndexPath + this.indexAccessor.keyPathSeperator() + indexName;
			InputStream inputStream = this.indexAccessor.readObject(path);
			
			result = MapperUtil.jsonMapper.readValue(inputStream, IndexWriteTarget.class);
			
			this.writeTargets.put(key.targetHash, result);
			
			inputStream.close();
		}
		
		return result;
	}

	private int scanIndexRange(long fromEpoch, long toEpoch) throws IOException {
		
		String prefix = options.prefix();		
		String searchAfter = String.valueOf(fromEpoch - 1);
		
		Iterator<List<String>> indexObjectIter = indexAccessor.iterateObjectKeys(
			prefix, searchAfter);
			
		int submittedTasks = 0;
		int submittedKeys = 0;
		
		while (indexObjectIter.hasNext()) {
			
			List<String> keys = indexObjectIter.next();
			
			if (keys.isEmpty()) {
				continue;
			}
			
			int endIndex;
			
			for (endIndex = 0; endIndex < keys.size(); endIndex++) {
				
				String key = keys.get(endIndex);
				
				IndexObjectKey objectKey = IndexObjectKey.fromPath(indexAccessor, prefix, key, false);
				
				long epochValue = Long.valueOf(objectKey.epoch);
				
				if (epochValue >= toEpoch) {
					break;
				}
			}
			
			if (endIndex > 0) {
				
				List<String> actualKeys = keys.subList(0, endIndex);
				
				executorService.submit(new IterateIndexObjectsTask(toEpoch, actualKeys));
				
				submittedKeys += actualKeys.size();
				submittedTasks++;
			}
			
			if (endIndex != keys.size()) {
				break;
			}
		}

		if (logger.isDebugEnabled()) {
			
			logger.debug("submitted {} iter index objects tasks, from {} -> to {} ({} keys)",
					submittedTasks, fromEpoch, toEpoch, submittedKeys);
		}
		
		return submittedKeys;
	}
	
	private void submitMatchingByteRanges(Map<String, Set<ByteRange>> byteRanges) throws JsonProcessingException {
		
		if ((byteRanges == null) ||
			(byteRanges.isEmpty())) {
			
			return;
		}
		
		QueryObjectsOptions mainRequest = createStreamRequest(byteRanges);

		Collection<QueryObjectsOptions> requests = this.splitStreamRequest(mainRequest);
	
		if (logger.isDebugEnabled()) {
			
			logger.debug("stream requests: " +
				MapperUtil.jsonMapper.writeValueAsString(requests));
		}
		
		for (QueryObjectsOptions request : requests) {
			
			String subStreamName = this.basePipelineName + STREAM_SUFFIX + "-" + this.subStreams.getAndIncrement();
			
			streamFunctionClient.send(request, this.launchRequest, streamRequest(subStreamName));
		}
	}

	@Override
	public synchronized void close() throws IOException {

		if (this.closed) {
			throw new IOException("closed");
		}

		try {

			submitQuery();

		} finally {

			if (this.executorService != null) {
				
				if (this.latch != null) {
					
					try {
						this.latch.await();
						
					} catch (InterruptedException e) {
					}
				}
				
				ExecutorUtil.safeAwaitTermination(this.executorService);
			}
			
			this.scanFunctionClient.close();

			if (this.streamFunctionClient != this.scanFunctionClient) {
				this.streamFunctionClient.close();
			}
			
			this.closed = true;
		}
		
		super.close();
	}
	
	private void submitQuery() {

		if (templateHashes.isEmpty()) {

			logger.warn("no matching template hashes to process");
			return;
		}

		if (logger.isDebugEnabled()) {

			logger.debug("templates hashes: " + this.templateHashes + " vars: " + this.vars);
		}

		IndexQueryOptions options = (IndexQueryOptions) this.options;

		try {

			long queryRange = options.queryFilterTo - options.queryFilterFrom;

			if ((this.timeslice != 0) &&
				(this.timeslice < queryRange)) {

				this.submitToEndpoint();

			} else {

				if (logger.isDebugEnabled()) {
					logger.debug("submitting to local thread pool");
				}

				this.submitToExecutor();
			}

		} catch (Exception e) {

			logger.error("error processing: " + this, e);
		}
	}
	
	protected void submitToEndpoint() {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;

		long currFrom = options.queryFilterFrom;
		long currTo = options.queryFilterFrom + this.timeslice;
		
		int index = 0;
		
		IndexQueryOptions baseScanRequest = new IndexQueryOptions(
				options.queryObjectStorageName,
				options.queryObjectStorageArgs,
				options.queryIndexContainer, 
				options.queryReadContainer,
				options.queryReadPrintProgress,
				options.queryFilterTerms, 
				new ArrayList<>(this.templateHashes), 
				new ArrayList<>(this.vars),
				options.queryFilterPrefix,
				currFrom,
				currTo,
				options.queryScanFlushInterval,
				options.queryScanFunctionUrl, 
				null,
				options.queryScanFunctionParallelMaxInstances,
				options.queryScanFunctionParallelThreads,
				options.queryStreamFunctionParallelObjects,
				options.queryStreamFunctionParallelByteRange,
				options.queryActions,
				options.queryStreamFunctionUrl);
		
		do {

			TimeSlice timeSliceOverrides = new TimeSlice(currFrom, currTo);

			String subQueryName = this.basePipelineName + SUB_QUERY_SUFFIX + "-" + index;
			
			scanFunctionClient.send(baseScanRequest, timeSliceOverrides, queryRequest(subQueryName));

			currFrom = currTo;
			currTo += this.timeslice;
			
			index++;

		} while (currFrom < options.queryFilterTo);

		if (logger.isDebugEnabled()) {
			logger.debug("tasks submitted to remote endpoint: " + index);
		}
	}
	
	protected void submitToExecutor() throws IOException {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;
		
		int scanTasks = options.queryScanFunctionParallelThreads;
		
		int threadPoolSize = (scanTasks > 1
				? (scanTasks + 1)
				: 1);

		if (logger.isDebugEnabled()) {
			logger.debug("scanTasks:" + scanTasks + ", threadPoolSize: " + threadPoolSize);
		}

		this.executorService = Executors.newFixedThreadPool(threadPoolSize);
		
		if (scanTasks > 1) {

			this.latch = new CountDownLatch(scanTasks);
			
			int submitted = 0;
			
			long timerange = options.queryFilterTo - options.queryFilterFrom;
			long taskTimeslice = (long)Math.ceil(timerange / (double)scanTasks);
			
			long taskFrom = options.queryFilterFrom;
			
			do {
				
				long taskTo = Math.min(
					taskFrom + taskTimeslice, 
					options.queryFilterTo
				);

				executorService.submit(
						new ScanIndexRangeTask(taskFrom, taskTo, submitted));
				
				taskFrom += taskTimeslice;
				submitted++;
				
			} while (taskFrom < options.queryFilterTo);
			
			int tasksDiff = scanTasks - submitted;
			
			for (int i = 0; i < tasksDiff; tasksDiff++) {
				this.latch.countDown();
			}
			
			logger.debug("submitted index range tasks: " + submitted);
		
		} else {
			
			logger.debug("executing sync");

			this.scanIndexRange(options.queryFilterFrom, options.queryFilterTo);
		}
	}
	
	private QueryObjectsOptions createStreamRequest(Map<String, Set<ByteRange>> indexObjects) {
		
		List<QueryObjectOptions> objectRequests = this.createObjectRequests(indexObjects);

		int size = objectRequests.size();

		List<QueryObjectOptions> expanded = new ArrayList<>(size * 2);
				
		for (int i = 0; i < size; i++) {
			
			QueryObjectOptions request = objectRequests.get(i);
			
			int rangeSize = request.size();
			
			long startOffset = request.offset(0);
			long endOffset = request.offset(rangeSize - 1) + request.length(rangeSize - 1);
			
			long byteRangeSpan = endOffset - startOffset;
			
			if ((rangeSize > 1) &&
				(this.byteRange != 0) &&
				(byteRangeSpan > this.byteRange)) {
				
				this.splitObjectRequest(request, expanded);
				
			} else {
				
				expanded.add(request);
			}		
		}
		
		return new QueryObjectsOptions(expanded);
	}
	
	private void splitObjectRequest(QueryObjectOptions request, List<QueryObjectOptions> output) {

		long requestStartPos = request.offset(0);
		int firstRequestIndex = 0;

		int size = request.size();

		for (int i = 1; i < size; i++) {

			long currentEndPos = request.offset(i) + request.length(i);
			long currentLength = currentEndPos - requestStartPos;

			if (currentLength > this.byteRange) {

				QueryObjectOptions subOptions = request.subOptions(firstRequestIndex, i);
				
				output.add(subOptions);

				firstRequestIndex = i;
				requestStartPos = request.offset(i);
			}
		}
		
		QueryObjectOptions subOptions = request.subOptions(firstRequestIndex, size);
		
		output.add(subOptions);
	}
 	
	private Collection<QueryObjectsOptions> splitStreamRequest(
		QueryObjectsOptions request) {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;
				
		if (options.queryStreamFunctionParallelObjects == 0) {
			return Collections.singleton(request);
		}
			
		int index = 0;
		int size = request.queryObject.size();
		
		Collection<QueryObjectsOptions> result = new ArrayList<>();
		
		while (index < size) {
			
			int remaining = size - index;
			int subListSize = Math.min(options.queryStreamFunctionParallelObjects, remaining);
						
			List<QueryObjectOptions> subList = request.queryObject.subList(index,
				index + subListSize);
			
			QueryObjectsOptions subRequest = new QueryObjectsOptions();
			
			subRequest.queryObject.addAll(subList);
			
			index += subListSize;
			
			result.add(subRequest);
		}
		
		return result;
	}
	
	private QueryObjectOptions createObjectRequest(String targetObject,
		int byteRangeSize, boolean filterTimestamps) {
		
		if (byteRangeSize == 0) {
			return null;
		}
		
		IndexQueryOptions queryOptions = (IndexQueryOptions)this.options;

		return new QueryObjectOptions(
			queryOptions.queryObjectStorageName, queryOptions.queryObjectStorageArgs,
			queryOptions.queryFilterTerms, targetObject, queryOptions.queryReadContainer,
			(filterTimestamps) ? queryOptions.queryFilterFrom : 0, 
			(filterTimestamps) ? queryOptions.queryFilterTo : 0,
			new long[byteRangeSize * 2]);
	}
	
	private List<QueryObjectOptions> createObjectRequests(Map<String, Set<ByteRange>> targetObjects) {
		
		List<QueryObjectOptions> result = new ArrayList<>();
		
		IndexQueryOptions queryOptions = (IndexQueryOptions)this.options;

		for (Map.Entry<String, Set<ByteRange>> entry : targetObjects.entrySet()) {
					
			String targetObject = entry.getKey();
			Collection<ByteRange> targetByteRanges = new TreeSet<>(ByteRangeComparator.INSTANCE);
			
			targetByteRanges.addAll(entry.getValue());
			
			int index = 0;		
			int rangesInTimeframe = 0;
			
			boolean[] rangesInTimeframeStates = new boolean[targetByteRanges.size()];
						
			for (ByteRange byteRange : targetByteRanges) {

				long minTimestamp = byteRange.minTimestamp;
				long maxTimestamp = byteRange.maxTimestamp;

				if ((queryOptions.queryFilterFrom <= minTimestamp) &&
					(queryOptions.queryFilterTo    > maxTimestamp)) {
					
					rangesInTimeframeStates[index] = true;
					rangesInTimeframe++;
				}
				
				index++;
			}
						
			int rangesOutsideTimeframe = targetByteRanges.size() - rangesInTimeframe;
			
			QueryObjectOptions insideTimeframeRequest = this.createObjectRequest(
					targetObject, rangesInTimeframe, false);

			QueryObjectOptions outsideTimeframeRequest = this.createObjectRequest(
					targetObject, rangesOutsideTimeframe, true);
			
			index = 0;
			
			for (ByteRange byteRange : targetByteRanges) {

				QueryObjectOptions request = rangesInTimeframeStates[index]
						? insideTimeframeRequest
						: outsideTimeframeRequest;

				long offset = byteRange.offset;
				int length = byteRange.length;

				request.add(offset, length);

				index++;
			}

			if (insideTimeframeRequest != null) {
				result.add(insideTimeframeRequest);
			}

			if (outsideTimeframeRequest != null) {
				result.add(outsideTimeframeRequest);
			}
		}
			
		return result;
	}
	
	@Override
	public String toString() {
		
		return String.format("IndexQueryWriter(%s)", this.options); 
	}
	
	protected static class TimeSlice {
		public final long queryFilterFrom;
		public final long queryFilterTo;
		
		protected TimeSlice(long from, long to) {
			this.queryFilterFrom = from;
			this.queryFilterTo = to;
		}
	}
}
