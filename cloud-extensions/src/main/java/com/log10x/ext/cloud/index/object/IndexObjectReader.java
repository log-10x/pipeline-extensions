package com.log10x.ext.cloud.index.object;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.TreeMap;

import com.log10x.ext.cloud.index.BaseIndexReader;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.util.reader.term.MultiTermFilterReader;
import com.log10x.ext.cloud.index.util.reader.term.SingleTermFilterReader;
import com.log10x.ext.cloud.index.util.stream.ByteRangeFilterInputStream;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * This class is used to read a KV object byte range from 
 * a target container (e.g. AWS S3 bucket). Lines read from input are
 * optionally filtered against a set of search terms.
 */
public class IndexObjectReader extends BaseIndexReader {

	private final Reader reader;

	public IndexObjectReader(IndexQueryObjectOptions options,
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {

		super(options, indexAccessor, evaluatorBean);

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
			options.queryObjectTarget, initialOffset, (int)rangeEnd);

		ByteRangeFilterInputStream inputStream = new ByteRangeFilterInputStream(storageStream,
			byteRanges);
				
		this.reader = this.createTermReader(inputStream);
	}
	
	private Reader createTermReader(InputStream inputStream) {
		
		IndexQueryObjectOptions options = (IndexQueryObjectOptions)this.options;
		
		switch (options.queryObjectTerms.size()) {
		
			case 0: return new InputStreamReader(inputStream);
			
			case 1: return new SingleTermFilterReader(inputStream,
						options.queryObjectTerms.get(0));
				
			default: return new MultiTermFilterReader(inputStream, 
					 	options.queryObjectTerms); 
		}	
	}
	
	public IndexObjectReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws IllegalArgumentException, IOException{
			
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
