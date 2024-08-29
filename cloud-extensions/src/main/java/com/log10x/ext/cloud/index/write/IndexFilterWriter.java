package com.log10x.ext.cloud.index.write;

import static com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.VALUES_TAG;

import java.io.IOException;
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

import com.log10x.ext.cloud.index.BaseIndexWriter;
import com.log10x.ext.cloud.index.filter.EncodedBloomFilter;
import com.log10x.ext.cloud.index.filter.EncodedBloomFilterBuilder;
import com.log10x.ext.cloud.index.filter.EncodedEventInput;
import com.log10x.ext.cloud.index.interfaces.IndexObjectKey;
import com.log10x.ext.cloud.index.interfaces.InputStreamLinebreaks;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.write.IndexWriteTarget.ByteRange;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.json.MapperUtil;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * This class is used to write a set of bloom filters for an input stream
 * with a specified accuracy to a target KV storage. To see how this class
 * is wired into the l1x pipeline see: {@link https://github.com/l1x-co/config/blob/main/pipelines/run/modules/input/objectStorage/index/stream.yaml}
 */
public class IndexFilterWriter extends BaseIndexWriter {

	private static final Logger logger = LogManager.getLogger(IndexFilterWriter.class);
	
	private static final boolean DEBUG_VALUES_TAG_ENABLED = true;
	private static final boolean INDEX_ALL_TIMESTAMPS = false;
	private static final boolean SINGLE_BUILDER_PER_CHUNK = false;
	private static final boolean USE_NOW_FOR_MISSING_TIMESTAMPS = false;
	
	protected class IndexBloomFilterBuilder extends EncodedBloomFilterBuilder {
		
		private final long resolutionEpoch;
		private final IndexObjectKey key;
		
		protected IndexBloomFilterBuilder(long resolutionEpoch, long targetHash, int byteRangeIndex) {
			
			super(((IndexWriteOptions)options).indexFetchErrorProb());
			
			this.resolutionEpoch = resolutionEpoch;
			
			this.key = new IndexObjectKey(
					options.prefix(), pathTimestamp(), targetHash, byteRangeIndex, null);
		}
		
		@Override
		protected int filterByteLen(String candidate) {
			
			String indexObjectPath = this.key.path(indexAccessor);
			
			String key = String.join(indexAccessor.keyPathSeperator(), indexObjectPath, candidate);
			
			return indexAccessor.keyByteLength(key);
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
			
			return String.format("Filters(%s)", builder.key.path(indexAccessor));
		}
	}
	
	protected class FinishWriteTask implements Runnable {
		
		@Override
		public void run() {
			latch.countDown();
		}
	}
	
	private final long epochResolution;
	
	private final long byteRangeLength;
	
	private final long now;

	private final Long2ObjectOpenHashMap<IndexBloomFilterBuilder> currBuilders;
	
	private final IndexWriteTarget metaIndex;
	
	private final IndexWriteReverseObject reverseIndex;
	
	private final EncodedEventInput currEventInput;
	
	private final IndexFilterStats filterStats;
	
	private final AtomicInteger indexBytes;
	
	private final AtomicInteger indexValues;
	
	private final AtomicInteger indexFilters;
	
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
	 * this constructor is invoked by the l1x runtime.
	 * 
	 * @param 	args
	 * 			the values of the 'IndexWrite' option group
	 * 		    instance for which the IndexFilterWriter is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an l1x evaluator bean which allows this input
	 * 			to interact with the l1x run-time 
	 */
	public IndexFilterWriter(Map<String, Object> args, EvaluatorBean evaluatorBean){
		
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexWriteOptions.class), null, evaluatorBean);
	}
	
	public IndexFilterWriter(IndexWriteOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) {
			
		super(options, indexAccessor, evaluatorBean);
		
		this.now = System.currentTimeMillis();
					
		this.filterStats = new IndexFilterStats();
		
		this.currBuilders = new Long2ObjectOpenHashMap<>();
		
		this.metaIndex = new IndexWriteTarget(options.indexReadObject, hash(options.indexReadObject));
		
		this.reverseIndex = new IndexWriteReverseObject(options.indexContainer());
        this.currEventInput = new EncodedEventInput();       
		
        this.epochResolution = this.parse("parseDuration", options.indexWriteResolution);    
		this.byteRangeLength = this.parse("parseBytes", options.indexWriteByteRange);    

		this.indexBytes = new AtomicInteger();
		this.indexValues = new AtomicInteger();	
		this.indexFilters = new AtomicInteger();	

		evaluatorBean.debugState("indexBytes", this.indexBytes);
		evaluatorBean.debugState("indexValues", this.indexValues);
		evaluatorBean.debugState("indexFilters", this.indexFilters);
		
		this.executorService = Executors.newSingleThreadExecutor();
		this.latch = new CountDownLatch(1);
		
		this.lastTimestamp = now;
	}
	
	private void writeFilters(IndexBloomFilterBuilder builder) throws IOException {
		
		List<EncodedBloomFilter> encodedFilters = builder.build();

		for (EncodedBloomFilter encodedFilter : encodedFilters) {
		
			String indexObjectPath = builder.key.path(indexAccessor);

			Map<String, String> tags;
			
			if ((DEBUG_VALUES_TAG_ENABLED) &&
				(encodedFilter.values != null)) {
				
				String filterValues = encodedFilter.values.toString();
				String base64Values = Base64.getEncoder().encodeToString(filterValues.getBytes());
				
				tags = Map.of(VALUES_TAG, base64Values);
						
			} else {
				
				tags = Collections.emptyMap();
			}
			
			String objectPath = indexAccessor.putIndexObject(indexObjectPath,
				encodedFilter.encodedFilter, null, tags);	
			
			filterStats.addFilter(encodedFilter.prob, 
				encodedFilter.byteLen, encodedFilter.elementSize);
			
			indexFilters.incrementAndGet();
			indexBytes.addAndGet(encodedFilter.byteLen);
			indexValues.addAndGet(encodedFilter.elementSize);
		
			reverseIndex.indexObjects.add(objectPath);
		}
		
		filterStats.addFilterGroup(encodedFilters.size());
	}	
	
	private IndexWriteOptions options() {
		return (IndexWriteOptions)this.options;
	}
	
	private InputStreamLinebreaks inputLinebreaks() {
		
		String key = options().key();
		
		Object value = evaluatorBean.dict(key);
		
		return (value instanceof InputStreamLinebreaks) ?
			(InputStreamLinebreaks)value :
			null;
	}
	
	private void appendToFilterBuilder(long epoch) throws IOException {
		
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
			resolutionEpoch = milliEpoch - (milliEpoch % this.epochResolution);
		}
		
		IndexBloomFilterBuilder builder = currBuilders.get(resolutionEpoch);
		
		if (builder == null) {
			
			builder = new IndexBloomFilterBuilder(
					resolutionEpoch, this.metaIndex.targetHash, this.currByteRangeIndex);
			
			currBuilders.put(resolutionEpoch, builder);
			
			filterStats.addEpoch(resolutionEpoch);			
		}
		
		builder.append(this.currEventInput, epoch);
	}
	
	/**
	 * append the current l1x Object to the target bloom filter builder
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

		if (currByteRangeLength > this.byteRangeLength) {
											
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
	
			long eventSequence = currEventInput.groupSequence - 1;
								
			this.currEventLineStart = linebreaks.pos(eventSequence);
			this.currEventSequence = eventSequence;
			
			if (currEventInput.timestamp.length > 0) {
				
				if (INDEX_ALL_TIMESTAMPS) {
				
					for (long timestamp : currEventInput.timestamp) {
						this.appendToFilterBuilder(timestamp);
						
						this.lastTimestamp = timestamp;
					}
					
				} else {
				
					this.appendToFilterBuilder(currEventInput.timestamp[0]);
					
					this.lastTimestamp = currEventInput.timestamp[0];
				}
	
			} else {
				
				long timestampToUse = (USE_NOW_FOR_MISSING_TIMESTAMPS
						? this.now
						: this.lastTimestamp);
				
				this.appendToFilterBuilder(timestampToUse);
			}
				
		} catch (Exception e) {
			
			logger.error("error flushing: " + this.currChars, e);
			
		} finally { 
			
			super.flush();
		}
	}
	
	private void writeByteRange(long start, int length) {

		ByteRange byteRange = new ByteRange(
				start, length, currMinByteRangeTimestamp, currMaxByteRangeTimestamp);

		ObjectIterator<Long2ObjectMap.Entry<IndexBloomFilterBuilder>> iter =
				currBuilders.long2ObjectEntrySet().fastIterator();

		while (iter.hasNext()) {

			Long2ObjectMap.Entry<IndexBloomFilterBuilder> entry = iter.next();

			IndexBloomFilterBuilder filterBuilder = entry.getValue();

			this.executorService.submit(new WriteFiltersTask(filterBuilder));
		}

		this.metaIndex.byteRanges.add(byteRange);

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
		
		try {
				
			long start = this.currByteRangeStart;
			long end = linebreaks.endPos();
			
			this.writeByteRange(start, (int)(end - start));
			
			if (this.executorService != null) {
				
				this.executorService.submit(new FinishWriteTask());
				
				this.executorService.shutdown();
				
				this.latch.await();
			}

			writeMetaIndex();
			writeReverseIndex();
			
			filterStats.logStats();
			
		} catch (Exception e) {
			logger.error("error closing: " + this, e);
			
		}
		
		super.close();
	}
	
	private void writeMetaIndex() throws IOException {

		if (this.metaIndex.byteRanges.isEmpty()) {
			return;
		}
		
		String metaIndexName = String.valueOf(this.metaIndex.targetHash);
		String metaIndexPath = this.indexAccessor.metaIndexPath(options().prefix());
		
		String body = MapperUtil.jsonMapper.writeValueAsString(this.metaIndex);
		
		if (logger.isDebugEnabled()) {
			logger.debug("meta index for " + this.metaIndex.target + ": " + body);
		}
		
		String path = indexAccessor.putObject(
				metaIndexPath, metaIndexName, body, Collections.emptyMap());
		
		reverseIndex.indexObjects.add(path);
	}
	
	private void writeReverseIndex() throws IOException {

		if (reverseIndex.indexObjects.isEmpty()) {
			return;
		}
		
		String reverseIndexName = options().indexReadObject;
		String reverseIndexPath = this.indexAccessor.reverseIndexPath(options().prefix());
		
		String body = MapperUtil.jsonMapper.writeValueAsString(this.reverseIndex);
		
		if (logger.isDebugEnabled()) {
			logger.debug("reverse index for " + reverseIndexName + ": " + body);
		}
		
		indexAccessor.putObject(reverseIndexPath,
				reverseIndexName, body, Collections.emptyMap());	
	}
	
	@Override
	public String toString() {
		
		return String.format("builders: %s\nreverseIndex: %s",
			currBuilders.toString(), 
			reverseIndex.indexObjects.toString());
	}

	private static long hash(String s) {
		long result = 0;

		for (int i = 0; i < s.length(); i++) {
			result = 31 * result + s.charAt(i);
		}

		return result;
	}
}
