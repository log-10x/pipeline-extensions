package com.log10x.ext.cloud.index;

import java.io.IOException;
import java.io.Reader;

import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.edge.bean.EvaluatorBean;

/**
 * A utility reader that composes an array of Readers created by any of its sub-classes
 * into a single Reader separated by a line break.
 */
public abstract class CompositeIndexReader extends BaseIndexReader {

	private final Reader[] readers;
	
	private int index;
	
	private int currRead;
	
	protected CompositeIndexReader(IndexContainerOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {
		
		super(options, indexAccessor, evaluatorBean);
		this.readers = this.createReaders();
	}
	
	protected abstract Reader[] createReaders() throws IOException;
	
	@Override
	public synchronized int read(char[] cbuf, int off, int len) throws IOException {
		
		if (this.index == readers.length) {
			return -1;
		}
		
		int read = this.readers[this.index].read(cbuf, off, len);
		
		if (read != -1) {
			
			this.currRead += read;
			return read;
		}
							
		this.index++;

		if (this.index == readers.length) {
			return -1;
		}
		
		boolean readLinebreak = this.currRead > 0;
		this.currRead = 0;
				 
		return (readLinebreak) ?
			readLineBreak(cbuf, off, len) :
			this.read(cbuf, off, len);
	}
	
	@Override
	public void close() throws IOException {
		
		for (Reader reader : this.readers) {
			reader.close();
		}
		
		super.close();
	}

}
