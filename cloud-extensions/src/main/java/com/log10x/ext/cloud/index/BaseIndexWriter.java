package com.log10x.ext.cloud.index;

import java.io.IOException;
import java.io.Writer;

import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.IndexEvaluatorAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.util.reader.StringBuilderReader;
import com.log10x.ext.edge.bean.EvaluatorBean;

/**
 * A utility base class into which l1x Object field values can be written
 * and whose descendants generate index objects (e.g. l1xTemplates, bloom filters)
 * that are written to an underlying KV storage (e.g. AWS S3). 
 */
public abstract class BaseIndexWriter extends Writer implements IndexEvaluatorAccessor {
				
	protected final IndexContainerOptions options;
	
	protected final EvaluatorBean evaluatorBean;
	
	protected final ObjectStorageIndexAccessor indexAccessor;
	
	private final boolean indexAccessorCreated;
	
	protected final StringBuilderReader currChars;

	protected BaseIndexWriter(IndexContainerOptions options, 
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
		
		this.currChars = new StringBuilderReader();	
	}
	
	@Override
	public EvaluatorBean evaluator() {
		return this.evaluatorBean;
	}
		
	@Override
	public synchronized void write(char[] cbuf, int off, int len) throws IOException {
		currChars.builder.append(cbuf, off, len);	
	}
	
	@Override
	public synchronized void flush() throws IOException {
		 currChars.reset();
	}

	@Override
	public synchronized void close() throws IOException {

		if (indexAccessorCreated) {
			this.indexAccessor.close();
		}
	}
}
