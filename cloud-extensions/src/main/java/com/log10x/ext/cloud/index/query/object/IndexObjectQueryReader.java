package com.log10x.ext.cloud.index.query.object;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.shared.BaseIndexReader;
import com.log10x.ext.cloud.index.util.stream.ByteRangeFilterInputStream;

/**
 * This class reads a KV object byte range from 
 * a target container (e.g. AWS S3 bucket). Lines read from input are
 * optionally filtered against a set of search terms.
 * 
 * To learn more see {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/object/stream.yaml}
 */
public class IndexObjectQueryReader extends BaseIndexReader {

	private static final Logger logger = LogManager.getLogger(IndexObjectQueryReader.class);

	private final Reader reader;

	public IndexObjectQueryReader(IndexQueryObjectOptions options,
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {

		super(options, indexAccessor, evaluatorBean);

		InputStream	inputStream = this.createInputStream(options);
		
		this.reader = this.createTermReader(inputStream);
	}
	
	private InputStream createInputStream(IndexQueryObjectOptions options) throws IOException {
		
		long now = System.currentTimeMillis();
		
		long elapseTime = options.elapseTime();
		
		if ((elapseTime != 0) &&
			(now > elapseTime)) {
			
			if (logger.isDebugEnabled()) {
				
				logger.debug("skipping query: " + options.ID() +
					"elapseTime:" + elapseTime + " exceeds: " + now);
			}
			
			return InputStream.nullInputStream();
		}
		
		int size = options.size();
		
		if (size == 0) {
			throw new IllegalArgumentException("byteRanges");
		}

		TreeMap<Long, Integer> byteRanges = new TreeMap<>();
		
		long initialOffset = Long.MAX_VALUE;
		
		for (int i = 0; i < size; i++) {
			initialOffset = Math.min(initialOffset, options.offset(i));			
		}
		
		for (int i = 0; i < size; i++) {
			
			long off = options.offset(i);
			int len = options.length(i);
			
			byteRanges.put(off - initialOffset, len);				
		}
		
		long rangeEnd = byteRanges.lastKey() + byteRanges.lastEntry().getValue();
		
		InputStream storageStream = this.indexAccessor.readObject(
			options.queryObjectTargetObject, initialOffset, (int)rangeEnd);

		return new ByteRangeFilterInputStream(storageStream,
			byteRanges);

	}
	
	protected Reader createTermReader(InputStream inputStream) {
		
		return new InputStreamReader(inputStream);
		
//		IndexQueryObjectOptions options = (IndexQueryObjectOptions)this.options;
//		
//		switch (options.queryObjectTerms.size()) {
//		
//			case 0: return new InputStreamReader(inputStream);
//			
//			case 1: return new SingleTermFilterReader(inputStream,
//						options.queryObjectTerms.get(0));
//				
//			default: return new MultiTermFilterReader(inputStream, 
//					 	options.queryObjectTerms); 
//		}	
	}
	
	public IndexObjectQueryReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws IllegalArgumentException, IOException{
			
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexQueryObjectOptions.class), null, evaluatorBean);
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		return reader.read(cbuf, off, len);
	}

	@Override
	public void close() throws IOException {
		
		reader.close();	
		super.close();
	}
}
