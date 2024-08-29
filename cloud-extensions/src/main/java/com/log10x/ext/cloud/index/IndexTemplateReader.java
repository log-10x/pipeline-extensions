package com.log10x.ext.cloud.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.util.reader.StringBuilderReader;
import com.log10x.ext.edge.bean.EvaluatorBean;

/**
 * This class is used to read JSON serialized l1x template values 
 * from a target KV storage container (e.g. AWS S3 bucket).
 */
public class IndexTemplateReader extends BaseIndexReader {
	
	private final Iterator<String> templateIter;
	
	private final String templatePath;
	
	private final String templateEnclosedPath;
	
	private final String templateInlinePath;

	protected final StringBuilderReader inlineReader;
	
	protected Reader objectReader;
	
	private boolean readPreviously;
		
	public IndexTemplateReader(IndexContainerOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {
			
		super(options, indexAccessor, evaluatorBean);
				
		this.inlineReader = new StringBuilderReader();
		
		String prefix = options.prefix();
		
		this.templatePath = this.indexAccessor.templatePath(prefix);
		this.templateEnclosedPath = this.indexAccessor.templateEnclosedPath(prefix);
		this.templateInlinePath = this.indexAccessor.templateInlinePath(prefix);

		Iterator<List<String>> indexIter = this.indexAccessor.iterateObjectKeys(
			this.templatePath, null);
		
		List<String> templateKeys = new ArrayList<>();
		
		while (indexIter.hasNext()) {
			templateKeys.addAll(indexIter.next());
		}
		
		this.templateIter = templateKeys.iterator();
	}
			
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		
		int result =  (this.objectReader != null) ?
			objectReader.read(cbuf, off, len) :
			inlineReader.read(cbuf, off, len);
		
		if (result > 0) {
			return result;
		}
					
		if (!templateIter.hasNext()) {
			return -1;
		}
			
		if (this.objectReader != null) {
			
			objectReader.close();
			this.objectReader = null;
		}
		
		String templateObject = templateIter.next();
		
		if (!templateObject.startsWith(this.templatePath)) {
			return 0;
		}
		
		if (templateObject.isEmpty()) {
			return this.read(cbuf, off, len);
		}
			
		if (templateObject.startsWith(this.templateEnclosedPath)) {
										
			InputStream stream = this.indexAccessor.readIndexObject(templateObject);
			this.objectReader = new InputStreamReader(stream);
			
		} else if (templateObject.startsWith(this.templateInlinePath)) {
			
			String indexObjectValue = this.indexAccessor.expandIndexObjectKey(templateObject);
			String inlineTemplate = indexObjectValue.substring(
					templateInlinePath.length() + this.indexAccessor.keyPathSeperator().length());			
			
			inlineReader.reset();
			inlineReader.builder.append(inlineTemplate);
			
		} else {
			
			throw new IllegalStateException("unsupported template: " + templateObject);
		}
		
		if (!this.readPreviously) {
			
			this.readPreviously = true;
			return this.read(cbuf, off, len);
		
		} else {
			
			return readLineBreak(cbuf, off, len);
		}
	}
	
	@Override
	public void close() throws IOException {	
		
		if (this.objectReader != null) {
			objectReader.close();
		}
		
		super.close();
	}
}
