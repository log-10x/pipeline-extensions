package com.log10x.ext.cloud.index;

import java.io.IOException;
import java.io.Reader;

import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.IndexEvaluatorAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.edge.bean.EvaluatorBean;

/**
 * A utility class that serves as base class for all l1x index Readers.
 */
public abstract class BaseIndexReader extends Reader implements IndexEvaluatorAccessor {
	
	protected final IndexContainerOptions options;
	
	protected final EvaluatorBean evaluatorBean;
	
	protected final ObjectStorageIndexAccessor indexAccessor;
	
	private final boolean indexAccessorCreated;
	
	protected BaseIndexReader(IndexContainerOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) {
		
		this.options = options;
		this.evaluatorBean = evaluatorBean;

		if (indexAccessor != null) {

			this.indexAccessor = indexAccessor;
			this.indexAccessorCreated = false;

		} else {

			this.indexAccessor = IndexAccessorUtil.createIndexAccessor(evaluatorBean,
				this.options);
			
			this.indexAccessorCreated = true;
		}
	}
	
	@Override
	public EvaluatorBean evaluator() {
		return this.evaluatorBean;
	}
			
	protected static int readLineBreak(char[] cbuf, int off, int len) {
			
		String lineSeparator = System.lineSeparator();
		
		int result = Math.min(lineSeparator.length(), len);	
		
		lineSeparator.getChars(0, result, cbuf, off);
		
		return result;
	}
	
	@Override
	public void close() throws IOException {
		
		if (indexAccessorCreated) {
			this.indexAccessor.close();
		}
	}
}
