package com.log10x.ext.cloud.index.shared;

import java.io.IOException;
import java.io.Writer;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.options.IndexContainerOptions;
import com.log10x.ext.edge.util.StringBuilderReader;

/**
 * A utility base class into which tenxObject field values can be written
 * and whose descendants generate index objects (e.g. tenxTemplates, bloom filters)
 * that are written to an underlying KV storage (e.g. AWS S3). 
 */
public abstract class BaseIndexWriter extends Writer {
				
	public static final byte[] LINE_BREAK_BYTES = System.lineSeparator().getBytes();
	
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

			this.indexAccessor = IndexAccessorFactory.createIndexAccessor(evaluatorBean, 
				this.options);
			
			this.indexAccessorCreated = true;
		}
		
		this.currChars = new StringBuilderReader();	
	}
	
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
