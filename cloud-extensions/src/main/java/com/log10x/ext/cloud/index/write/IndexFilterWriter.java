package com.log10x.ext.cloud.index.write;

import static com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.DEBUG_VALUES_TAG;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.filter.EncodedBloomFilter;
import com.log10x.ext.cloud.index.filter.EncodedBloomFilterBuilder;
import com.log10x.ext.cloud.index.filter.EncodedEventInput;
import com.log10x.ext.cloud.index.interfaces.InputStreamLinebreaks;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;
import com.log10x.ext.cloud.index.shared.BaseIndexWriter;
import com.log10x.ext.cloud.index.shared.IndexFilterKey;
import com.log10x.ext.cloud.index.shared.TokenSplitter;
import com.log10x.ext.cloud.index.write.TargetObjectByteRangeIndex.TimestampByteRange;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * This class is used to write a set of bloom filters for an input stream
 * with a specified accuracy to a target KV storage. To see how this class
 * is wired into the 10x pipeline see: {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/index/stream.yaml}
 */
public class IndexFilterWriter extends BaseIndexWriter {

	private static final Logger logger = LogManager.getLogger(IndexFilterWriter.class);
	
	private static final boolean DEBUG_VALUES_TAG_ENABLED = true;
	private static final boolean INDEX_ALL_TIMESTAMPS = false;
	private static final boolean SINGLE_BUILDER_PER_CHUNK = false;
	private static final boolean USE_NOW_FOR_MISSING_TIMESTAMPS = false;
	
	protected class IndexBloomFilterBuilder extends EncodedBloomFilterBuilder {
		
		private final long resolutionEpoch;
		private final IndexFilterKey indexFilterKey;
		
		protected IndexBloomFilterBuilder(long resolutionEpoch, String targetHash, int byteRangeIndex) {
			
			super(tokenSplitter, ((IndexWriteOptions)options).indexFetchErrorProb());
			
			this.resolutionEpoch = resolutionEpoch;
			
			this.indexFilterKey = IndexFilterKey.from(
				options.target(), this.pathTimestamp(), 
				targetHash, byteRangeIndex, null);
		}
		
		@Override
		protected int filterByteLen(String filterCandidate) {
			
			String filterPath = indexFilterKey.formatPath(indexAccessor);
			
			String filterKey = String.join(indexAccessor.keyPathSeperator(), 
				filterPath, filterCandidate);
			
			return indexAccessor.keyByteLength(filterKey);
		}
		
		protected long pathTimestamp() {
			
			return (this.resolutionEpoch == 0
					? this.minEpoch
					: this.resolutionEpoch);
		}
	}
	
	protected class WriteFiltersTask implements Runnable {

		private final IndexBloomFilterBuilder builder;
		
		protected WriteFiltersTask(IndexBloomFilterBuilder builder) {
			
			this.builder = builder;
		}

		@Override
		public void run() {
			
			String threadName = Thread.currentThread().getName();
			
			try {
				
				Thread.currentThread().setName(this.getClass().getSimpleName() + 
					" " + this.toString());
				
				writeFilters(builder);	
				
			} catch (Exception e) {
				
				logger.error("error writing index filters: " + this, e);
				
			} finally {
				
				Thread.currentThread().setName(threadName);
			}
		}
		
		@Override
		public String toString() {
			
			return builder.indexFilterKey.formatPath(indexAccessor);
		}
	}
	
	protected class FinishWriteTask implements Runnable {
		
		@Override
		public void run() {
			latch.countDown();
		}
	}
		
	private final long now;

	private final Long2ObjectOpenHashMap<IndexBloomFilterBuilder> currBuilders;
	
	private final TargetObjectByteRangeIndex byteRangeIndex;
	
	private final TargetObjectReverseIndex reverseIndex;
	
	private final EncodedEventInput currEventInput;
	
	private final IndexFilterStats filterStats;
	
	private final AtomicInteger indexBytes;
	
	private final AtomicInteger indexValues;
	
	private final AtomicInteger indexFilters;
	
	private final TokenSplitter tokenSplitter;
	
	private InputStreamLinebreaks linebreaks;
	
	private int currByteRangeIndex;
	private long currByteRangeStart;
	private long currMinByteRangeTimestamp;
	private long currMaxByteRangeTimestamp;

	private long currEventLineStart;
	
	private long currEventSequence;
	
	private long lastTimestamp;
	
	private final ExecutorService executorService;
	private final CountDownLatch latch;
	
	private volatile boolean closed;

	/**
	 * this constructor is invoked by the 10x engine.
	 * 
	 * @param 	args
	 * 			the values of the 'IndexWrite' option group
	 * 		    instance for which the IndexFilterWriter is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this input
	 * 			to interact with the 10x run-time 
	 * @throws IllegalArgumentException 
	 * @throws NoSuchAlgorithmException 
	 */
	public IndexFilterWriter(Map<String, Object> args, EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException, IllegalArgumentException{
		
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexWriteOptions.class), null, evaluatorBean);
	}
	
	public IndexFilterWriter(IndexWriteOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException {
			
		super(options, indexAccessor, evaluatorBean);
		
		this.now = System.currentTimeMillis();
					
		this.filterStats = new IndexFilterStats();
		
		this.currBuilders = new Long2ObjectOpenHashMap<>();
		
		MessageDigest digest = MessageDigest.getInstance("MD5");
		
		byte[] bytes = digest.digest(options.indexReadObject.getBytes());
		
		String targetHash = Base64.getUrlEncoder().encodeToString(bytes);
				
		this.byteRangeIndex = new TargetObjectByteRangeIndex(options.indexReadObject, targetHash);		
		this.reverseIndex = new TargetObjectReverseIndex();
        
		this.currEventInput = new EncodedEventInput();       

		this.indexBytes = new AtomicInteger();
		this.indexValues = new AtomicInteger();
		this.indexFilters = new AtomicInteger();

		evaluatorBean.debugState("indexBytes", this.indexBytes);
		evaluatorBean.debugState("indexValues", this.indexValues);
		evaluatorBean.debugState("indexFilters", this.indexFilters);

		this.tokenSplitter = TokenSplitter.create(evaluatorBean);

		this.executorService = Executors.newSingleThreadExecutor();
		this.latch = new CountDownLatch(1);

		this.lastTimestamp = now;
	}
	
	private void writeFilters(IndexBloomFilterBuilder builder) throws IOException {
		
		List<EncodedBloomFilter> encodedFilters = builder.build();

		for (EncodedBloomFilter encodedFilter : encodedFilters) {
		
			String indexFilterPath = builder.indexFilterKey.formatPath(this.indexAccessor);

			Map<String, String> tags;
			
			if ((DEBUG_VALUES_TAG_ENABLED) &&
				(encodedFilter.values != null)) {
				
				String filterValues = encodedFilter.values.toString();
				String base64Values = Base64.getEncoder().encodeToString(filterValues.getBytes());
				
				tags = Map.of(DEBUG_VALUES_TAG, base64Values);
						
			} else {
				
				tags = Collections.emptyMap();
			}
			
			String outputObjectKey = indexAccessor.putObject(indexFilterPath,
				encodedFilter.encodedFilter, null, 0L, tags);	
			
			filterStats.addFilter(encodedFilter.prob, 
				encodedFilter.byteLen, encodedFilter.elementSize);
			
			indexFilters.incrementAndGet();
			indexBytes.addAndGet(encodedFilter.byteLen);
			indexValues.addAndGet(encodedFilter.elementSize);
		
			reverseIndex.indexObjects.add(outputObjectKey);
		}
		
		filterStats.addFilterGroup(encodedFilters.size());
	}	
	
	private IndexWriteOptions options() {
		return (IndexWriteOptions)this.options;
	}
	
	private InputStreamLinebreaks inputLinebreaks() {
		
		String key = options().key();
		
		Object value = evaluatorBean.get(key);
		
		return (value instanceof InputStreamLinebreaks) ?
			(InputStreamLinebreaks)value :
			null;
	}
	
	private void appendToFilterBuilder(long timestamp) throws IOException {
		
		long epoch;

		if (timestamp > 0) {
			epoch = timestamp;
		} else {
			epoch = (USE_NOW_FOR_MISSING_TIMESTAMPS ? this.now : this.lastTimestamp);
		}

		this.currMinByteRangeTimestamp = (this.currMinByteRangeTimestamp > 0) ?
				Math.min(this.currMinByteRangeTimestamp, epoch) :
				epoch;
		
		this.currMaxByteRangeTimestamp = (this.currMaxByteRangeTimestamp > 0) ?
				Math.max(this.currMaxByteRangeTimestamp, epoch) :
				epoch;
		
		long resolutionEpoch;
		
		if (SINGLE_BUILDER_PER_CHUNK) {
			
			resolutionEpoch = 0;
			
		} else {
			
			long milliEpoch = TimeUnit.MILLISECONDS.toMillis(epoch);
			resolutionEpoch = milliEpoch - (milliEpoch % options().indexWriteResolution);
		}
		
		IndexBloomFilterBuilder builder = currBuilders.get(resolutionEpoch);
		
		if (builder == null) {
			
			builder = new IndexBloomFilterBuilder(
				resolutionEpoch, this.byteRangeIndex.targetHash, 
				this.currByteRangeIndex);
			
			currBuilders.put(resolutionEpoch, builder);
			
			filterStats.addEpoch(resolutionEpoch);			
		}
		
		builder.append(this.currEventInput, epoch);
	}
	
	/**
	 * append the current tenxObject to the target bloom filter builder
	 */
	@Override
	public synchronized void flush() throws IOException {

		if (this.closed) {
			throw new IllegalStateException("closed");
		}
		
		if (this.linebreaks == null) {
			this.linebreaks = this.inputLinebreaks();
		}
		
		long currByteRangeLength = this.currEventLineStart - this.currByteRangeStart;

		if (currByteRangeLength > options().indexWriteByteRange) {
											
			this.writeByteRange(this.currByteRangeStart, (int)currByteRangeLength - 1);			
			
			this.currByteRangeStart = this.currEventLineStart;
			
			linebreaks.deleteTo(this.currEventSequence);
		}

		if (currChars.builder.isEmpty()) {
			return;
		}
		
		try {
		
			MapperUtil.jsonMapper.
				readerForUpdating(this.currEventInput).
				readValue(this.currChars);
	
			// Epoch timestamps can be in several different resolutions.
			// Align all to MS so there are no inconsistencies later in the process.
			//
			for (int i = 0; i < currEventInput.timestamp.length; i++) {
				long timestampMs = toEpochMilli(currEventInput.timestamp[i]);
				
				currEventInput.timestamp[i] = timestampMs;
			}
			
			long eventSequence = currEventInput.groupSequence - 1;
								
			this.currEventLineStart = linebreaks.pos(eventSequence);
			this.currEventSequence = eventSequence;

			if (currEventInput.timestamp.length > 0) {

				if (INDEX_ALL_TIMESTAMPS) {

					for (long timestamp : currEventInput.timestamp) {
						this.appendToFilterBuilder(timestamp);

						if (timestamp > 0) {
							this.lastTimestamp = timestamp;
						}
					}

				} else {

					this.appendToFilterBuilder(currEventInput.timestamp[0]);

					if (currEventInput.timestamp[0] > 0) {
						this.lastTimestamp = currEventInput.timestamp[0];
					}
				}

			} else {

				this.appendToFilterBuilder(0);
			}
				
		} catch (Exception e) {
			
			logger.error("error flushing: " + this.currChars, e);
			
		} finally { 
			
			super.flush();
		}
	}

	private static long toEpochMilli(long epochTime) {
		// Try milliseconds first
		if (epochTime > 1_000_000_000_000L && epochTime < 10_000_000_000_000L) {
			return epochTime; // Likely milliseconds
		}
		// Try seconds
		if (epochTime > 1_000_000_000L && epochTime < 10_000_000_000L) {
			return epochTime * 1000; // Convert seconds to milliseconds
		}
		// Try microseconds
		if (epochTime > 1_000_000_000_000_000L && epochTime < 10_000_000_000_000_000L) {
			return epochTime / 1000; // Convert microseconds to milliseconds
		}
		// Try nanoseconds
		if (epochTime > 1_000_000_000_000_000_000L) {
			return epochTime / 1_000_000; // Convert nanoseconds to milliseconds
		}
		throw new IllegalArgumentException("Invalid epoch time: " + epochTime);
	}

	private void writeByteRange(long start, int length) {

		TimestampByteRange byteRange = new TimestampByteRange(start, length, 
			this.currMinByteRangeTimestamp, this.currMaxByteRangeTimestamp);

		ObjectIterator<Long2ObjectMap.Entry<IndexBloomFilterBuilder>> iter =
			currBuilders.long2ObjectEntrySet().fastIterator();

		while (iter.hasNext()) {

			Long2ObjectMap.Entry<IndexBloomFilterBuilder> entry = iter.next();

			IndexBloomFilterBuilder filterBuilder = entry.getValue();

			executorService.submit(new WriteFiltersTask(filterBuilder));
		}

		byteRangeIndex.byteRanges.add(byteRange);

		this.currByteRangeIndex++;
		this.currBuilders.clear();
	}
	
	/**
	 * commit bloom filters to the KV storage
	 */
	@Override
	public synchronized void close() throws IOException {
		
		if (this.closed) {
			throw new IOException("closed");
		}
		
		this.closed = true;
		
		if (this.linebreaks != null) {
		
			try {
					
				long start = this.currByteRangeStart;
				long end = linebreaks.endPos();
				
				this.writeByteRange(start, (int)(end - start));
				
				if (this.executorService != null) {
					
					this.executorService.submit(new FinishWriteTask());
					
					this.executorService.shutdown();
					
					this.latch.await();
				}
	
				this.writeByteRangeIndex();
				this.writeReverseIndex();

				filterStats.logStats();

				if (logger.isInfoEnabled()) {
					long minTs = Long.MAX_VALUE;
					long maxTs = Long.MIN_VALUE;
					for (TimestampByteRange br : byteRangeIndex.byteRanges) {
						if (br.minTimestamp > 0 && br.minTimestamp < minTs) minTs = br.minTimestamp;
						if (br.maxTimestamp > maxTs) maxTs = br.maxTimestamp;
					}
					logger.info("index written: object={}, byteRanges={}, filters={}, values={}, " +
						"minTimestamp={} ({}), maxTimestamp={} ({})",
						byteRangeIndex.target,
						byteRangeIndex.byteRanges.size(),
						indexFilters.get(),
						indexValues.get(),
						minTs, new java.util.Date(minTs),
						maxTs, new java.util.Date(maxTs));
				}
				
			} catch (Exception e) {
				
				logger.error("error closing: " + this, e);
				
			}
		}
		
		super.close();
	}
	
	private void writeByteRangeIndex() throws IOException {

		if (byteRangeIndex.byteRanges.isEmpty()) {
			return;
		}
		
		String byteRangeIndexPath = indexAccessor.indexObjectPath(IndexObjectType.byteRange, 
			options().target());
		
		String body = MapperUtil.jsonMapper.writeValueAsString(this.byteRangeIndex);
		
		if (logger.isDebugEnabled()) {
			logger.debug("byte range index for: " + byteRangeIndex.target + ": " + body);
		}
		
		byte[] bytes = body.getBytes();
		
		String path = indexAccessor.putObject(
			byteRangeIndexPath, String.valueOf(byteRangeIndex.targetHash), 
			new ByteArrayInputStream(bytes), bytes.length,
			Collections.emptyMap()
		);
		
		reverseIndex.indexObjects.add(path);
	}
	
	private void writeReverseIndex() throws IOException {

		if (reverseIndex.indexObjects.isEmpty()) {
			return;
		}

		String reverseIndexPath = indexAccessor.indexObjectPath(IndexObjectType.reverseIndex,
			options().target());

		String reverseIndexTarget = options().indexReadObject;
		
		String body = MapperUtil.jsonMapper.writeValueAsString(this.reverseIndex);
		
		if (logger.isDebugEnabled()) {
			logger.debug("reverse index for: " + reverseIndexTarget + ": " + body);
		}
		
		byte[] bytes = body.getBytes();
		
		indexAccessor.putObject(reverseIndexPath,
			reverseIndexTarget, new ByteArrayInputStream(bytes), bytes.length,
			Collections.emptyMap()
		);	
	}
	
	@Override
	public String toString() {
		
		return String.format("builders: %s\nreverseIndex: %s",
			currBuilders.toString(), 
			reverseIndex.indexObjects.toString());
	}
}
