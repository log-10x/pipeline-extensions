package com.log10x.ext.cloud.index.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.pipeline.endpoint.PipelineLaunchRequest;
import com.log10x.api.pipeline.launch.PipelineLaunchOptions;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.client.IndexQueryClient;
import com.log10x.ext.cloud.index.filter.DecodedBloomFilter;
import static com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.MDC_QUERY_ID;

import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.QueryLogLevel;
import com.log10x.ext.cloud.index.shared.BaseIndexWriter;
import com.log10x.ext.cloud.index.shared.IndexFilterKey;
import com.log10x.ext.cloud.index.shared.TokenSplitter;
import com.log10x.ext.cloud.index.util.ExecutorUtil;
import com.log10x.ext.cloud.index.write.TargetObjectByteRangeIndex;
import com.log10x.ext.cloud.index.write.TargetObjectByteRangeIndex.TimestampByteRange;

/**
 * This class is used to produce a set of requests to a remote end-point 
 * to fetch a target set of input objects byte ranges residing
 * within a KV storage and scan them for events falling within 
 * a specific time range and matching a set of search terms.
 * To learn more about how this class is instantiated by a host 10x pipeline, see: 
 * {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/query/stream.yaml# }
 */
public class IndexQueryWriter extends BaseIndexWriter {
	
	protected static final YAMLMapper yamlMapper = MapperUtil.initMapper(YAMLMapper.builder());

	private static final Logger logger = LogManager.getLogger(IndexQueryWriter.class);
	
	// Subquery is a wrapper config, not an actual app on it's own
	private static final PipelineLaunchRequest SUBQUERY =
			PipelineLaunchRequest.newBuilder()
				.withBootstrapArg(PipelineLaunchOptions.RUN_TIME_NAME, "myObjectStorageSubQuery")
				.withIncludes("cloud/streamer/subquery", "gitops")
				.build();
	
	// Stream is an app where the user can tweak the config
	private static final PipelineLaunchRequest STREAM =
			PipelineLaunchRequest.newBuilder().withTenX("@/apps/cloud/streamer/stream").build();
	
	private static final String VAR_SEPERATOR = ",";
	
	protected static class ByteRangeComparator implements Comparator<TimestampByteRange> {
		
		protected static final ByteRangeComparator INSTANCE = new ByteRangeComparator();
		
		@Override
		public int compare(TimestampByteRange o1, TimestampByteRange o2) {
			
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
				logQuery(QueryLogLevel.ERROR,
						String.format("scan error: failed iterating index objects (%s): %s",
						this, e.getMessage()));

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

			} catch (Exception e) {

				logger.error("error scanning index range: " + this, e);
				logQuery(QueryLogLevel.ERROR,
						String.format("scan error: failed scanning index range (%s): %s",
						this, e.getMessage()));

			} finally {

				latch.countDown();
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
	
	private final List<List<String>> vars;
	
	private final String queryId;
	
	private final long queryElapseTime;
	
	private long lastQueryProcessingCheck;

	private final IndexQueryClient scanFunctionClient;
	
	private final IndexQueryClient streamFunctionClient;
	
	private final long timeslice;
	
	private final String basePipelineUuid;
		
	private final Map<String, TargetObjectByteRangeIndex> blobByteRangeIndexMap;
	private final Set<String> submittedKeyHashes;
	private final Object keyHashesLock;
	
	private final TokenSplitter tokenSplitter;
	
	private ExecutorService executorService;
	private CountDownLatch latch;
	
	private volatile boolean closed;

	private long queryStartTime;

	// Aggregate scan counters (summed across all IterateIndexObjectsTask runs for this writer).
	// Used to emit a per-query 'scan summary' event and to enrich 'query complete' with
	// actionable classification data — distinguishes 0-result queries whose Bloom scans
	// ran but matched nothing (bloom-miss) from queries whose index range returned no blobs
	// (stale-indexer / empty-range).
	private final AtomicLong aggScanned = new AtomicLong();
	private final AtomicLong aggMatched = new AtomicLong();
	private final AtomicLong aggSkippedSearch = new AtomicLong();
	private final AtomicLong aggSkippedTemplate = new AtomicLong();
	private final AtomicLong aggSkippedDuplicate = new AtomicLong();
	private final AtomicLong aggSubmittedTasks = new AtomicLong();
	private final AtomicLong aggSubmittedKeys = new AtomicLong();
	private final AtomicLong aggStreamRequests = new AtomicLong();
	private final AtomicLong aggStreamObjects = new AtomicLong();
	private final AtomicLong aggStreamBlobs = new AtomicLong();

	private boolean shouldLog(QueryLogLevel level) {
		IndexQueryOptions opts = (IndexQueryOptions) this.options;
		List<String> levels = opts.queryLogLevels();
		if (levels == null || levels.isEmpty()) {
			return level != QueryLogLevel.DEBUG;
		}
		return levels.contains(level.name());
	}

	private void logQuery(QueryLogLevel level, String message) {
		if (!shouldLog(level)) return;
		this.indexAccessor.logQueryEvent(this.queryId, this.basePipelineUuid, level, message, traceContextMetadata(null));
	}

	private void logQuery(QueryLogLevel level, String message, Map<String, Object> metadata) {
		if (!shouldLog(level)) return;
		this.indexAccessor.logQueryEvent(this.queryId, this.basePipelineUuid, level, message, traceContextMetadata(metadata));
	}

	/**
	 * O13 — auto-inject W3C trace context (traceparent / tracestate) from
	 * SLF4J MDC into the structured CW log payload's `data` block. Lets
	 * downstream observability tools correlate sub-query worker events to
	 * the originating REST request without grep-on-queryId-then-grep-again.
	 * Returns the metadata map verbatim (or a fresh map) with trace fields
	 * added when present.
	 */
	private static Map<String, Object> traceContextMetadata(Map<String, Object> existing) {
		String tp = org.slf4j.MDC.get("traceparent");
		String ts = org.slf4j.MDC.get("tracestate");
		if ((tp == null || tp.isEmpty()) && (ts == null || ts.isEmpty())) {
			return existing;
		}
		Map<String, Object> out = (existing != null) ? new java.util.LinkedHashMap<>(existing) : new java.util.LinkedHashMap<>();
		if (tp != null && !tp.isEmpty()) out.put("traceparent", tp);
		if (ts != null && !ts.isEmpty()) out.put("tracestate", ts);
		return out;
	}

	/**
	 * this constructor is invoked by the 10x run-time.
	 * 
	 * @param 	args
	 * 			a map of arguments passed to the 10x cli for the
	 *  		target output for which this stream is instantiated
	 * 			containing user configuration of this query
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this output
	 * 			to interact with the 10x run-time
	 */
	public IndexQueryWriter(Map<String, Object> args, EvaluatorBean evaluatorBean) 
		throws IllegalArgumentException{
			
		this(MapperUtil.jsonMapper(evaluatorBean).convertValue(args,
			IndexQueryOptions.class), null, evaluatorBean);
	}

	public IndexQueryWriter(IndexQueryOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) {

		super(options, indexAccessor, evaluatorBean);

		validateOptions(options);

		this.currInputValue = new IndexQueryEvent();

		this.templateHashes = new HashSet<String>(options.queryFilterTemplateHashes);

		this.vars = new ArrayList<>(options.queryFilterVars.size());

		for (String queryFilterVar : options.queryFilterVars) {
			vars.add(Arrays.asList(queryFilterVar.split(VAR_SEPERATOR)));
		}

		this.queryId = (options.queryId != null && !options.queryId.isBlank()) ?
			options.queryId :
			UUID.randomUUID().toString();

		if (logger.isDebugEnabled()) {
			logger.debug("query init: search={}, filterVars={}, filterTemplateHashes={}, queryId={}",
				options.querySearch,
				options.queryFilterVars != null ? options.queryFilterVars.size() : 0,
				options.queryFilterTemplateHashes != null ? options.queryFilterTemplateHashes.size() : 0,
				this.queryId);
		}

		ThreadContext.put(MDC_QUERY_ID, this.queryId);

		if (options.queryLimitProcessingTime != 0) {
		
			this.queryElapseTime =  (options.queryElapseTime != 0) ?
				options.queryElapseTime :
				System.currentTimeMillis() + options.queryLimitProcessingTime;
			
		} else {
			
			this.queryElapseTime = 0;
		}

		this.scanFunctionClient = new IndexQueryClient(this.indexAccessor, options.queryScanFunctionUrl, evaluatorBean);	
		
		this.streamFunctionClient = options.queryScanFunctionUrl.equals(options.queryStreamFunctionUrl) ?
			this.scanFunctionClient :
			new IndexQueryClient(this.indexAccessor, options.queryStreamFunctionUrl, evaluatorBean);
		
		this.timeslice = calcTimeSlice(options);
		
		this.basePipelineUuid = (String)this.evaluator().env(PipelineLaunchOptions.UNIQUE_ID);
		
		this.blobByteRangeIndexMap = new HashMap<>();
		this.submittedKeyHashes = new HashSet<>();
		this.keyHashesLock = new Object();
		
		this.tokenSplitter = TokenSplitter.create(evaluatorBean);
	}
	
	private static void validateOptions(IndexQueryOptions options) {

		if ((options.querySearch != null) &&
			(options.querySearch.isBlank())) {
			
			throw new IllegalArgumentException("search cannot be blank");
		}
		
		long timerange = options.queryTo - options.queryFrom;
		
		if (timerange <= 0) {
			
			throw new IllegalArgumentException("illegal time range, 'from': " + 
				options.queryFrom + " cannt be greater than 'to': " + options.queryTo);
		}
		
		int maxFunctionInstances = options.queryScanFunctionParallelMaxInstances;

		if (maxFunctionInstances <= 0) {

			throw new IllegalArgumentException("max instances must be positive. value:" + maxFunctionInstances);

		} else if (maxFunctionInstances > IndexQueryOptions.MAX_FUNCTION_INSTANCES_CAP) {

			throw new IllegalArgumentException("max instances can't be over: "
					+ IndexQueryOptions.MAX_FUNCTION_INSTANCES_CAP + ". value:" + maxFunctionInstances);
		}
		
		int threadCount = options.queryScanFunctionParallelThreads;
		
		if (threadCount < 0) {
			throw new IllegalArgumentException("parallel threads can't be negative. value:" + threadCount);
		}
	}

	private static long calcTimeSlice(IndexQueryOptions options) {
		
		long baseTimeSlice = options.queryScanFunctionParallelTimeslice;
		
		if (baseTimeSlice <= 0) {
			return 0;
		}
		
		int maxFunctionInstances = options.queryScanFunctionParallelMaxInstances;
		
		long timerange = options.queryTo - options.queryFrom;
		
		if (baseTimeSlice * maxFunctionInstances >= timerange) {
			return baseTimeSlice;
		}
		
		int timeSliceMultiplier = (int)Math.ceil(timerange / (double)(baseTimeSlice * maxFunctionInstances));
		
		return timeSliceMultiplier * baseTimeSlice;
	}
	
	@Override
	public synchronized void flush() throws IOException {
		
		if (currChars.builder.isEmpty()) {
			return;
		}
		
		try {

			this.currInputValue.nullify();

			MapperUtil.jsonMapper.
				readerForUpdating(this.currInputValue).
				readValue(this.currChars);

			if (currInputValue.templateHash != null) {

				templateHashes.add(currInputValue.templateHash);

			} else {

				List<String> inputVars = new LinkedList<>();

				if ((currInputValue.enrichmentValues != null) &&
					(currInputValue.enrichmentValues.length > 0)) {

					for (String enrichmentValue : currInputValue.enrichmentValues) {

						this.tokenSplitter.fill(enrichmentValue, inputVars);
					}
				} else if (currInputValue.vars != null) {

					for (Object rawVar : currInputValue.vars) {

						String stringVar = rawVar.toString();

						inputVars.add(stringVar);
					}
				} else if (logger.isDebugEnabled()) {
					logger.debug("flush: no templateHash, enrichmentValues, or vars. raw={}",
						this.currChars.builder.toString().substring(0, Math.min(200, this.currChars.builder.length())));
				}

				vars.add(inputVars);
			}

		} catch (Exception e) {

			logger.error("error flushing: " + this.currChars, e);
			logQuery(QueryLogLevel.ERROR,
					String.format("query error: failed parsing input event: %s", e.getMessage()));

		} finally {
			
			super.flush();
		}
	}
	
	protected class IndexDecodedBloomFilter extends DecodedBloomFilter 
		implements Function<Integer, Boolean> {

		public IndexDecodedBloomFilter(String encodedFilter) {
			super(encodedFilter);
		}

		@Override
		public Boolean apply(Integer index) {

			if (index >= vars.size()) {
				logger.warn("BloomFilter.apply: index {} >= vars.size {}",  index, vars.size());
				return false;
			}

			List<String> indexVars = vars.get(index);

			for (String indexVar : indexVars) {

				if (!this.test(indexVar)) {
					return false;
				}
			}

			if (indexVars.isEmpty() && logger.isDebugEnabled()) {
				logger.debug("BloomFilter.apply: empty vars at index={}, returning true (vacuous)", index);
			}

			return true;
		}		
	}
		
	private void iterateIndexObjects(long toEpoch,
		Collection<String> indexObjectKeys) throws IOException {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;
		
		Set<String> localLubmittedKeyHashes = new HashSet<>();
		Map<String, Set<TimestampByteRange>> output = new LinkedHashMap<>();

		String prefix = options.target();

		long lastSubmit = System.currentTimeMillis();
		long lastEpoch = -1;

		int scannedKeys = 0;
		int skippedDuplicate = 0;
		int skippedSearchFilter = 0;
		int skippedTemplateHash = 0;
		int matchedKeys = 0;
		
		QueryFilterEvaluator eval = (options.querySearch != null) ?
			QueryFilterEvaluator.parse(options.querySearch) :
			null;
		
		for (String indexObjectKey : indexObjectKeys) {

			try {

				scannedKeys++;

				IndexFilterKey key = IndexFilterKey.parseKey(this.indexAccessor,
					prefix, indexObjectKey);

				long epochValue = Long.valueOf(key.epoch);

				if (epochValue >= toEpoch) {
					logQuery(QueryLogLevel.DEBUG,
						String.format("scan: epoch %d >= toEpoch %d, stopping iteration", epochValue, toEpoch));
					break;
				}
				
				String submitHash = String.valueOf(key.targetHash) + "_" + String.valueOf(key.byteRangeIndex);
				
				if (localLubmittedKeyHashes.contains(submitHash)) {
					skippedDuplicate++;
					continue;
				}
				
				// We only want to submit once we scanned all of objects for a
				// given epoch value, to prevent a case where multiple 'stream'
				// functions stream the same timestamp.
				//
				if ((options.queryScanFlushInterval > 0) &&
					(epochValue != lastEpoch)) {
					
					long now = System.currentTimeMillis();
					
					if (now - lastSubmit > options.queryScanFlushInterval) {
						
						this.submitMatchingByteRanges(output);
						output.clear();
						
						lastSubmit = System.currentTimeMillis();
					}
				}
				
				lastEpoch = epochValue;
				
				String encodedFilter = key.encodedFilter;

				IndexDecodedBloomFilter filter = new IndexDecodedBloomFilter(encodedFilter);

				if ((eval != null) &&
					(!eval.evaluate(filter))) {

					skippedSearchFilter++;
					// #1 Per-blob bloom decision. DEBUG-gated so default runs stay
					// quiet. Escalate via queryLogLevels=[...,DEBUG] to discover
					// WHICH blobs passed/failed which filter — the difference
					// between "Bloom rejected every value" and "Bloom matched N"
					// that the aggregate counters collapse today.
					logQuery(QueryLogLevel.DEBUG,
						String.format("bloom tested: blob=%s, decision=skippedSearch, epoch=%d",
							indexObjectKey, epochValue),
						Map.of("blobKey", indexObjectKey, "epoch", epochValue,
							"decision", "skippedSearch",
							"templateHashesChecked", this.templateHashes.size()));
					continue;
				}

				if ((!templateHashes.isEmpty()) &&
					(!filter.testAny(this.templateHashes))) {

					skippedTemplateHash++;
					logQuery(QueryLogLevel.DEBUG,
						String.format("bloom tested: blob=%s, decision=skippedTemplate, epoch=%d",
							indexObjectKey, epochValue),
						Map.of("blobKey", indexObjectKey, "epoch", epochValue,
							"decision", "skippedTemplate",
							"templateHashesChecked", this.templateHashes.size()));
					continue;
				}

				TargetObjectByteRangeIndex blobByteRangeIndex;

				matchedKeys++;
				localLubmittedKeyHashes.add(submitHash);
				logQuery(QueryLogLevel.DEBUG,
					String.format("bloom tested: blob=%s, decision=matched, epoch=%d",
						indexObjectKey, epochValue),
					Map.of("blobKey", indexObjectKey, "epoch", epochValue,
						"decision", "matched",
						"templateHashesChecked", this.templateHashes.size()));

				synchronized (this.keyHashesLock) {
					
					if (!this.submittedKeyHashes.add(submitHash)) {
						continue;
					}
					
					blobByteRangeIndex = this.blobByteRangeIndex(key);
				}
				
				if (blobByteRangeIndex == null) {
					throw new IllegalStateException("missing byte range index: " + key);
				}
				
				Set<TimestampByteRange> timestampByteRanges = output.get(blobByteRangeIndex.target);
				
				if (timestampByteRanges == null) {
					
					timestampByteRanges = new HashSet<>();
					output.put(blobByteRangeIndex.target, timestampByteRanges);
				}
				
				TimestampByteRange byteRange = blobByteRangeIndex.byteRanges.get(key.byteRangeIndex);
				
				timestampByteRanges.add(byteRange);
			
			} catch (Exception e) {

				logger.error("error testing filter: " + indexObjectKey, e);
				logQuery(QueryLogLevel.ERROR,
						String.format("scan error: failed processing index key %s: %s",
						indexObjectKey, e.getMessage()));
				continue;
			}

		}
		
		this.submitMatchingByteRanges(output);

		aggScanned.addAndGet(scannedKeys);
		aggMatched.addAndGet(matchedKeys);
		aggSkippedDuplicate.addAndGet(skippedDuplicate);
		aggSkippedSearch.addAndGet(skippedSearchFilter);
		aggSkippedTemplate.addAndGet(skippedTemplateHash);

		logQuery(QueryLogLevel.PERF,
			String.format("scan complete: scanned=%d, matched=%d, skippedDuplicate=%d, skippedSearch=%d, skippedTemplate=%d",
				scannedKeys, matchedKeys, skippedDuplicate, skippedSearchFilter, skippedTemplateHash),
			Map.of("scanned", scannedKeys, "matched", matchedKeys,
				"skippedDuplicate", skippedDuplicate, "skippedSearch", skippedSearchFilter,
				"skippedTemplate", skippedTemplateHash));
	}

	private TargetObjectByteRangeIndex blobByteRangeIndex(IndexFilterKey key) throws IOException {
		
		TargetObjectByteRangeIndex result = blobByteRangeIndexMap.get(key.targetHash);
		
		if (result == null) {
			
			String indexPath = indexAccessor.indexObjectPath(IndexObjectType.byteRange, options.target());
			
			String blobIndexPath = indexPath + indexAccessor.keyPathSeperator() + key.targetHash;
			
			InputStream inputStream = indexAccessor.readObject(blobIndexPath);
			
			result = MapperUtil.jsonMapper.readValue(inputStream, TargetObjectByteRangeIndex.class);
			
			blobByteRangeIndexMap.put(key.targetHash, result);
			
			inputStream.close();
		}
		
		return result;
	}

	private int scanIndexRange(long fromEpoch, long toEpoch) throws IOException {
		
		String prefix = options.target();		
		String searchAfter = String.valueOf(fromEpoch - 1);
		
		Iterator<List<String>> indexObjectIter = indexAccessor.iterateObjectKeys(prefix, searchAfter, true);
			
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
								
				long epochValue = IndexFilterKey.parseEpoch(this.indexAccessor,
					prefix, key);
				
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

		aggSubmittedTasks.addAndGet(submittedTasks);
		aggSubmittedKeys.addAndGet(submittedKeys);

		// Always emit a range-level summary — critical for diagnosing "0 events" queries.
		// When submittedTasks=0, no IterateIndexObjectsTask will run, so 'scan complete'
		// would never fire; without this line the CW trace for an empty-range query has no
		// signal distinguishing "index prefix empty" from "scan found no matches".
		logQuery(QueryLogLevel.INFO,
				String.format("scan range: fromEpoch=%d, toEpoch=%d, submittedTasks=%d, submittedKeys=%d",
				fromEpoch, toEpoch, submittedTasks, submittedKeys),
				Map.of("fromEpoch", fromEpoch, "toEpoch", toEpoch,
						"submittedTasks", submittedTasks, "submittedKeys", submittedKeys));

		return submittedKeys;
	}
	
	private void submitMatchingByteRanges(Map<String, Set<TimestampByteRange>> byteRanges) throws IOException {
		
		if ((byteRanges == null) ||
			(byteRanges.isEmpty())) {
			
			return;
		}
		
		QueryObjectsOptions mainRequest = this.createStreamRequest(byteRanges);

		Collection<QueryObjectsOptions> requests = this.splitStreamRequest(mainRequest);

		if (logger.isDebugEnabled()) {

			logger.debug("stream requests: " +
				MapperUtil.jsonMapper.writeValueAsString(requests));
		}

		int totalObjects = 0;
		for (QueryObjectsOptions req : requests) {
			totalObjects += req.queryObject.size();
		}

		aggStreamRequests.addAndGet(requests.size());
		aggStreamObjects.addAndGet(totalObjects);
		aggStreamBlobs.addAndGet(byteRanges.size());

		logQuery(QueryLogLevel.PERF,
				String.format("stream dispatch: %d requests, %d objects, %d target blobs",
				requests.size(), totalObjects, byteRanges.size()),
				Map.of("requests", requests.size(), "objects", totalObjects, "blobs", byteRanges.size()));

		for (QueryObjectsOptions request : requests) {

			if (this.queryElapsed(true)) {
				break;
			}

			PipelineLaunchRequest streamRequest = PipelineLaunchRequest.newBuilder()
					.withBootstrapArg(PipelineLaunchOptions.PARENT_ID, this.basePipelineUuid)
					.build();

			// #2 Per stream-request assembly. Lets us see what byte ranges the
			// coordinator actually generated per dispatched request — degenerate
			// ranges (0-length, duplicate, unaligned) would explain the
			// "stream worker complete: fetched 0 bytes" symptom we see today.
			if (shouldLog(QueryLogLevel.DEBUG)) {
				int rangeCount = 0;
				long totalRangeBytes = 0;
				for (QueryObjectOptions qoo : request.queryObject) {
					if (qoo.byteRanges != null) {
						rangeCount += qoo.byteRanges.length / 2;
						for (int i = 1; i < qoo.byteRanges.length; i += 2) {
							totalRangeBytes += qoo.byteRanges[i];
						}
					}
				}
				String firstTarget = request.queryObject.isEmpty() ? "none" : request.queryObject.get(0).target;
				logQuery(QueryLogLevel.DEBUG,
					String.format("stream request: target=%s, objects=%d, byteRanges=%d, totalBytes=%d",
						firstTarget, request.queryObject.size(), rangeCount, totalRangeBytes),
					Map.of("target", firstTarget,
						"objects", request.queryObject.size(),
						"byteRanges", rangeCount,
						"totalBytes", totalRangeBytes));
			}

			streamFunctionClient.send(streamRequest, request, STREAM);
		}
	}
	
	private boolean queryElapsed(boolean checkResultLimit) throws IOException {
			
		IndexQueryOptions options = (IndexQueryOptions)this.options;

		long now = System.currentTimeMillis();
		
		if ((this.queryElapseTime != 0) &&
			(now > this.queryElapseTime)) {

			logger.info("aborting query {}: processing time limit exceeded", this.queryId);

			logQuery(QueryLogLevel.ERROR,
					String.format("query aborted: processing time limit exceeded (limit=%dms)",
					options.queryLimitProcessingTime));

			return true;
		}
		
		if (!checkResultLimit) {
			return false;
		}
		
		if (options.queryLimitResultSize == 0) {
			return false;
		}
		
		if (this.lastQueryProcessingCheck == 0) {
			
			this.lastQueryProcessingCheck = now;
			return false;
		}
			
		if (now - this.lastQueryProcessingCheck < options.queryScanFunctionLimitResultSizeInterval) {
			return false;
		}
		
		String basePath = indexAccessor.indexObjectPath(IndexObjectType.query, this.options.target());
		
		String queryPath = (new StringBuilder(basePath))
				.append(indexAccessor.keyPathSeperator())
				.append(this.queryId)
				.toString();
		
		Iterator<List<String>> iter = indexAccessor.iterateObjectKeys(queryPath, null, true);
		
		long currSize = 0;
		
		while (iter.hasNext()) {
			
			List<String> keys = iter.next();
			
			for (String key : keys) {
				
				String expandedKey = indexAccessor.expandIndexObjectKey(key);
				
				int lastPathIndex = expandedKey.lastIndexOf(indexAccessor.keyPathSeperator());
				
				if ((lastPathIndex == -1) || (lastPathIndex == expandedKey.length() - 1)) {
					continue;
				}
				
				String utfSizeValue = expandedKey.substring(lastPathIndex + 1);
				
				long utfSize = Long.parseLong(utfSizeValue);
				
				currSize += utfSize;
				
				if (currSize >= options.queryLimitResultSize) {

					logger.info("aborting query {}: result size limit exceeded ({} >= {})",
							this.queryId, currSize, options.queryLimitResultSize);

					logQuery(QueryLogLevel.ERROR,
							String.format("query aborted: result size limit exceeded (%d bytes >= %d bytes limit)",
							currSize, options.queryLimitResultSize),
							Map.of("currentBytes", currSize, "limitBytes", options.queryLimitResultSize));

					return true;
				}
				
			}
			
			this.lastQueryProcessingCheck = System.currentTimeMillis();
			
			return false;
		}
		 
		return false;	
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

			if (this.queryStartTime != 0) {
				long elapsed = System.currentTimeMillis() - this.queryStartTime;
				long scanned = aggScanned.get();
				long matched = aggMatched.get();
				long skippedSearch = aggSkippedSearch.get();
				long skippedTemplate = aggSkippedTemplate.get();
				long streamRequests = aggStreamRequests.get();
				long streamBlobs = aggStreamBlobs.get();
				long submittedTasks = aggSubmittedTasks.get();

				// Classification reason — enum consumed by MCP streamer-diagnostics to
				// distinguish 'we scanned and matched' vs the 3 distinct 0-result paths.
				String reason;
				if (matched > 0 && streamRequests > 0) {
					reason = "success";
				} else if (submittedTasks == 0) {
					reason = "empty-range";       // index prefix had no blobs for this window
				} else if (scanned > 0 && matched == 0) {
					reason = "bloom-miss";        // Bloom scans ran, zero matched
				} else if (matched > 0 && streamRequests == 0) {
					reason = "match-no-dispatch"; // matched but no stream requests dispatched
				} else {
					reason = "unknown";
				}

				logQuery(QueryLogLevel.PERF,
						String.format("query complete: elapsed=%dms, scanned=%d, matched=%d, " +
							"skippedSearch=%d, skippedTemplate=%d, streamRequests=%d, streamBlobs=%d, reason=%s",
							elapsed, scanned, matched, skippedSearch, skippedTemplate,
							streamRequests, streamBlobs, reason),
						Map.of("elapsedMs", elapsed,
							"scanned", scanned,
							"matched", matched,
							"skippedSearch", skippedSearch,
							"skippedTemplate", skippedTemplate,
							"streamRequests", streamRequests,
							"streamBlobs", streamBlobs,
							"submittedTasks", submittedTasks,
							"reason", reason));
			}

			this.closed = true;
		}

		try {
			super.close();
		} finally {
			ThreadContext.remove(MDC_QUERY_ID);
		}
	}

	private boolean isEmptyQuery() {

		if (!this.templateHashes.isEmpty()) {
			return false;
		}

		for (List<String> current : this.vars) {

			if (!current.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	private void submitQuery() {

		if (isEmptyQuery()) {
			logQuery(QueryLogLevel.INFO,
				String.format("query empty: no matching template hashes or vars (templateHashes=%d, vars=%d)",
					this.templateHashes.size(), this.vars.size()),
				Map.of("reason", "no_template_hashes_or_vars",
					"templateHashCount", this.templateHashes.size(),
					"varsCount", this.vars.size()));
			return;
		}

		if (logger.isDebugEnabled()) {

			logger.debug("templates hashes: " + this.templateHashes + " vars: " + this.vars);
		}

		IndexQueryOptions options = (IndexQueryOptions) this.options;

		this.queryStartTime = System.currentTimeMillis();

		long queryRange = options.queryTo - options.queryFrom;

		logQuery(QueryLogLevel.INFO,
				String.format("query started: name=%s, search=%s, target=%s, range=[%d,%d] (%dms), " +
				"processingTimeLimit=%dms, resultSizeLimit=%d",
				options.queryName, options.querySearch, options.queryTarget,
				options.queryFrom, options.queryTo, queryRange,
				options.queryLimitProcessingTime, options.queryLimitResultSize));

		boolean isRemoteDispatch = (this.timeslice != 0) && (this.timeslice < queryRange);

		logQuery(QueryLogLevel.INFO,
				String.format("query plan: templateHashes=%d, vars=%d, timeslice=%dms, dispatch=%s",
				this.templateHashes.size(), this.vars.size(), this.timeslice,
				isRemoteDispatch ? "remote" : "local"),
				Map.of("templateHashes", this.templateHashes.size(),
					"vars", this.vars.size(),
					"timeslice", this.timeslice,
					"dispatch", isRemoteDispatch ? "remote" : "local"));

		try {

			if (isRemoteDispatch) {

				this.submitToEndpoint();

			} else {

				if (logger.isDebugEnabled()) {
					logger.debug("submitting to local thread pool");
				}

				this.submitToExecutor();
			}

		} catch (Exception e) {

			logger.error("error processing: " + this, e);
			logQuery(QueryLogLevel.ERROR, "query error: " + e.getMessage());
		}
	}
	
	protected void submitToEndpoint() throws IOException {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;

		long currFrom = options.queryFrom;
		long currTo = options.queryFrom + this.timeslice;
		
		int index = 0;
		
		List<String> requestVars = new ArrayList<>(this.vars.size());
		
		for (List<String> varsArray : this.vars) {
			requestVars.add(String.join(VAR_SEPERATOR, varsArray));		
		}
		
		IndexQueryOptions baseScanRequest = new IndexQueryOptions(
				options.queryName,
				options.queryObjectStorageName,
				options.queryObjectStorageArgs,
				options.queryIndexContainer,
				options.queryReadContainer,
				options.queryReadPrintProgress,
				options.querySearch,
				options.queryFilters,
				new ArrayList<>(this.templateHashes),
				requestVars,
				this.queryId, options.queryLimitResultSize, this.queryElapseTime,
				options.queryLimitProcessingTime, options.queryScanFunctionLimitResultSizeInterval,
				options.queryTarget,
				currFrom,
				currTo,
				options.queryScanFlushInterval,
				options.queryScanFunctionUrl,
				0,
				options.queryScanFunctionParallelMaxInstances,
				options.queryScanFunctionParallelThreads,
				options.queryStreamFunctionParallelObjects,
				options.queryStreamFunctionParallelByteRange,
				options.queryStreamFunctionUrl,
				options.queryLogLevels,
				options.queryLogGroup,
				options.queryWriteResults);

		do {

			if (this.queryElapsed(false)) {
				break;
			}
			
			TimeSlice timeSliceOverrides = new TimeSlice(currFrom, currTo);

			PipelineLaunchRequest scanRequest = PipelineLaunchRequest.newBuilder()
					.withBootstrapArg(PipelineLaunchOptions.PARENT_ID, this.basePipelineUuid)
					.build();
			
			scanFunctionClient.send(scanRequest, baseScanRequest, SUBQUERY, timeSliceOverrides);

			currFrom = currTo;
			currTo += this.timeslice;
			
			index++;

		} while (currFrom < options.queryTo);

		if (logger.isDebugEnabled()) {
			logger.debug("tasks submitted to remote endpoint: " + index);
		}

		logQuery(QueryLogLevel.INFO,
				String.format("scan dispatched: %d remote scan tasks",
				index));
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
			
			long timerange = options.queryTo - options.queryFrom;
			long taskTimeslice = (long)Math.ceil(timerange / (double)scanTasks);
			
			long taskFrom = options.queryFrom;
			
			do {
				
				long taskTo = Math.min(
					taskFrom + taskTimeslice, 
					options.queryTo
				);

				executorService.submit(
						new ScanIndexRangeTask(taskFrom, taskTo, submitted));
				
				taskFrom += taskTimeslice;
				submitted++;
				
			} while (taskFrom < options.queryTo);
			
			int tasksDiff = scanTasks - submitted;
			
			for (int i = 0; i < tasksDiff; tasksDiff++) {
				this.latch.countDown();
			}
			
			logger.debug("submitted index range tasks: " + submitted);

			logQuery(QueryLogLevel.INFO,
					String.format("scan dispatched: %d local scan tasks, threads=%d", submitted, threadPoolSize));

		} else {

			logger.debug("executing sync");

			logQuery(QueryLogLevel.DEBUG, "scan dispatched: single-threaded local scan");

			this.scanIndexRange(options.queryFrom, options.queryTo);
		}
	}
	
	private QueryObjectsOptions createStreamRequest(Map<String, Set<TimestampByteRange>> indexObjects) {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;

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
				(options.queryStreamFunctionParallelByteRange != 0) &&
				(byteRangeSpan > options.queryStreamFunctionParallelByteRange)) {
				
				this.splitObjectRequest(request, expanded);
				
			} else {
				
				expanded.add(request);
			}		
		}
		
		return new QueryObjectsOptions(expanded);
	}
	
	private void splitObjectRequest(QueryObjectOptions request, List<QueryObjectOptions> output) {

		IndexQueryOptions options = (IndexQueryOptions)this.options;

		long requestStartPos = request.offset(0);
		int firstRequestIndex = 0;

		int size = request.size();

		for (int i = 1; i < size; i++) {

			long currentEndPos = request.offset(i) + request.length(i);
			long currentLength = currentEndPos - requestStartPos;

			if (currentLength > options.queryStreamFunctionParallelByteRange) {

				QueryObjectOptions subOptions = request.subOptions(firstRequestIndex, i);
				
				output.add(subOptions);

				firstRequestIndex = i;
				requestStartPos = request.offset(i);
			}
		}
		
		QueryObjectOptions subOptions = request.subOptions(firstRequestIndex, size);
		
		output.add(subOptions);
	}
 	
	/**
	 * SQS maximum message size is 256KB (262,144 bytes).
	 * We use a safety margin to account for message overhead and encoding.
	 */
	private static final int SQS_MAX_MESSAGE_SIZE_BYTES = 150_000;

	/**
	 * Splits a stream request into multiple smaller requests to stay within SQS message size limits.
	 *
	 * First splits by object count (queryStreamFunctionParallelObjects), then checks each
	 * resulting message against the SQS size limit and re-splits if necessary.
	 */
	private Collection<QueryObjectsOptions> splitStreamRequest(
		QueryObjectsOptions request) {

		IndexQueryOptions options = (IndexQueryOptions)this.options;

		// First pass: split by object count if configured
		Collection<QueryObjectsOptions> countSplitResults;

		if (options.queryStreamFunctionParallelObjects == 0) {
			countSplitResults = Collections.singleton(request);
		} else {
			countSplitResults = splitByObjectCount(request, options.queryStreamFunctionParallelObjects);
		}

		// Second pass: ensure each message is within SQS size limits
		Collection<QueryObjectsOptions> result = new ArrayList<>();

		for (QueryObjectsOptions countSplitRequest : countSplitResults) {
			Collection<QueryObjectsOptions> sizeSplitResults = splitByMessageSize(countSplitRequest);
			result.addAll(sizeSplitResults);
		}

		return result;
	}

	/**
	 * Splits requests by object count.
	 */
	private static Collection<QueryObjectsOptions> splitByObjectCount(QueryObjectsOptions request, int maxObjects) {

		int index = 0;
		int size = request.queryObject.size();

		Collection<QueryObjectsOptions> result = new ArrayList<>();

		while (index < size) {

			int remaining = size - index;
			int subListSize = Math.min(maxObjects, remaining);

			List<QueryObjectOptions> subList = request.queryObject.subList(index,
				index + subListSize);

			QueryObjectsOptions subRequest = new QueryObjectsOptions();

			subRequest.queryObject.addAll(subList);

			index += subListSize;

			result.add(subRequest);
		}

		return result;
	}

	/**
	 * Splits a request into multiple requests if the JSON serialization exceeds SQS size limits.
	 * Uses iterative size calculation: pre-computes each object's size once (O(n) serializations)
	 * then uses arithmetic to determine batch sizes, avoiding O(n²) re-serialization.
	 */
	private static Collection<QueryObjectsOptions> splitByMessageSize(QueryObjectsOptions request) {

		// Quick check: if the full message is within limits, return as-is
		int fullSize = calculateJsonSize(request);

		if (fullSize <= SQS_MAX_MESSAGE_SIZE_BYTES) {
			return Collections.singleton(request);
		}

		if (logger.isInfoEnabled()) {
			logger.info("Message size {} bytes exceeds limit {} bytes, splitting {} objects",
				fullSize, SQS_MAX_MESSAGE_SIZE_BYTES, request.queryObject.size());
		}

		// Pre-calculate size of each individual object once (O(n) serializations)
		int[] objectSizes = new int[request.queryObject.size()];
		for (int i = 0; i < request.queryObject.size(); i++) {
			objectSizes[i] = calculateObjectJsonSize(request.queryObject.get(i));
		}

		// Calculate base wrapper size: {"queryObject":[]}
		int baseWrapperSize = calculateJsonSize(new QueryObjectsOptions());

		Collection<QueryObjectsOptions> result = new ArrayList<>();
		QueryObjectsOptions currentBatch = new QueryObjectsOptions();
		int currentBatchSize = baseWrapperSize;

		for (int i = 0; i < request.queryObject.size(); i++) {
			QueryObjectOptions objectOption = request.queryObject.get(i);
			int objectSize = objectSizes[i];

			// Calculate projected size: current + comma (if not first in batch) + objectSize
			int commaSize = currentBatch.queryObject.isEmpty() ? 0 : 1;
			int projectedSize = currentBatchSize + commaSize + objectSize;

			if (projectedSize > SQS_MAX_MESSAGE_SIZE_BYTES) {

				// Current batch would exceed limit, finalize it (if non-empty) and start new batch
				if (!currentBatch.queryObject.isEmpty()) {
					result.add(currentBatch);
					currentBatch = new QueryObjectsOptions();
				}

				// Add this object to the new batch
				currentBatch.queryObject.add(objectOption);
				currentBatchSize = baseWrapperSize + objectSize;

				// If a single object exceeds the limit, log a warning but still include it
				if (currentBatchSize > SQS_MAX_MESSAGE_SIZE_BYTES) {
					logger.warn("Single object exceeds SQS size limit: {} bytes", currentBatchSize);
				}

			} else {
				// Object fits, add it to current batch
				currentBatch.queryObject.add(objectOption);
				currentBatchSize = projectedSize;
			}
		}

		// Add final batch if non-empty
		if (!currentBatch.queryObject.isEmpty()) {
			result.add(currentBatch);
		}

		if (logger.isInfoEnabled()) {
			logger.info("Split message into {} parts", result.size());
		}

		return result;
	}

	/**
	 * Calculates the JSON serialization size in bytes for a QueryObjectsOptions wrapper.
	 */
	private static int calculateJsonSize(QueryObjectsOptions request) {
		try {
			byte[] json = MapperUtil.jsonMapper.writeValueAsBytes(request);
			return json.length;
		} catch (JsonProcessingException e) {
			logger.error("Failed to calculate JSON size", e);
			return Integer.MAX_VALUE;
		}
	}

	/**
	 * Calculates the JSON serialization size in bytes for a single QueryObjectOptions.
	 */
	private static int calculateObjectJsonSize(QueryObjectOptions object) {
		try {
			byte[] json = MapperUtil.jsonMapper.writeValueAsBytes(object);
			return json.length;
		} catch (JsonProcessingException e) {
			logger.error("Failed to calculate object JSON size", e);
			return Integer.MAX_VALUE;
		}
	}
	
	private QueryObjectOptions createObjectRequest(String targetObject,
		int byteRangeSize, boolean filterTimestamps) {
		
		if (byteRangeSize == 0) {
			return null;
		}
		
		IndexQueryOptions queryOptions = (IndexQueryOptions)this.options;

		return new QueryObjectOptions(
				queryOptions.queryName, queryOptions.queryObjectStorageName,
				queryOptions.queryObjectStorageArgs, queryOptions.filter(),
				queryOptions.target(), targetObject,
				queryOptions.queryReadContainer, queryOptions.queryIndexContainer,
				(filterTimestamps) ? queryOptions.queryFrom : 0,
				(filterTimestamps) ? queryOptions.queryTo : 0,
				new long[byteRangeSize * 2], this.queryId, this.queryElapseTime,
				queryOptions.queryLogLevels(), queryOptions.queryLogGroup,
				queryOptions.queryWriteResults);
	}
	
	private List<QueryObjectOptions> createObjectRequests(Map<String, Set<TimestampByteRange>> targetObjects) {
		
		List<QueryObjectOptions> result = new ArrayList<>();
		
		IndexQueryOptions queryOptions = (IndexQueryOptions)this.options;

		for (Map.Entry<String, Set<TimestampByteRange>> entry : targetObjects.entrySet()) {
					
			String targetObject = entry.getKey();
			Collection<TimestampByteRange> targetByteRanges = new TreeSet<>(ByteRangeComparator.INSTANCE);
			
			targetByteRanges.addAll(entry.getValue());
			
			int index = 0;		
			int rangesInTimeframe = 0;
			
			boolean[] rangesInTimeframeStates = new boolean[targetByteRanges.size()];
						
			for (TimestampByteRange byteRange : targetByteRanges) {

				long minTimestamp = byteRange.minTimestamp;
				long maxTimestamp = byteRange.maxTimestamp;

				// Intersection semantics: the byte range counts as "in the
				// query's timeframe" if its [min, max] overlaps [from, to) at
				// all. Previously required strict containment, which fluent-bit
				// 10-minute blobs can never satisfy for 60s shard windows and
				// so routed everything through outsideTimeframeRequest +
				// per-event filter — adding unnecessary per-event work and
				// depending on `(!this.timestamp)` short-circuits to admit
				// events with no parsed timestamp.
				if ((queryOptions.queryFrom <= maxTimestamp) &&
					(queryOptions.queryTo    >  minTimestamp)) {

					rangesInTimeframeStates[index] = true;
					rangesInTimeframe++;
				}
				
				index++;
			}
						
			int rangesOutsideTimeframe = targetByteRanges.size() - rangesInTimeframe;

			logQuery(QueryLogLevel.DEBUG,
				String.format("timestamp split: object=%s, byteRanges=%d, inTimeframe=%d, outsideTimeframe=%d",
					targetObject, targetByteRanges.size(), rangesInTimeframe, rangesOutsideTimeframe));

			QueryObjectOptions insideTimeframeRequest = this.createObjectRequest(
					targetObject, rangesInTimeframe, false);

			QueryObjectOptions outsideTimeframeRequest = this.createObjectRequest(
					targetObject, rangesOutsideTimeframe, true);
			
			index = 0;
			
			for (TimestampByteRange byteRange : targetByteRanges) {

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
		
		public final long queryFrom;
		public final long queryTo;
		
		protected TimeSlice(long from, long to) {
			this.queryFrom = from;
			this.queryTo = to;
		}
	}
}
