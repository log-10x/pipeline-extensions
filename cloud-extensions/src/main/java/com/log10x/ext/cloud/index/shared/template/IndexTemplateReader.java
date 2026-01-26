package com.log10x.ext.cloud.index.shared.template;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.options.IndexContainerOptions;
import com.log10x.ext.cloud.index.shared.BaseIndexReader;

/**
 * This class is used to read JSON serialized TenxTemplate values 
 * from a target KV storage container (e.g. AWS S3 bucket).
 */
public class IndexTemplateReader extends BaseIndexReader {
	
	private final List<String> templateKeys;
	
	private int currTemplateKeyIndex;
		
	protected Reader currTemplateReader;
			
	public IndexTemplateReader(IndexContainerOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {
			
		super(options, indexAccessor, evaluatorBean);
						
		String prefix = options.target();
		
		IndexTemplates scanner = IndexTemplates.scan(this.indexAccessor, 
			prefix, false);
		
		this.templateKeys = scanner.currTemplateKeys;
	}
				
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		
		int templateKeysSize = templateKeys.size();
		
		if (this.currTemplateReader == null) {
			
			if (templateKeysSize == 0) {
				return -1;
			}
			
			String key = templateKeys.get(this.currTemplateKeyIndex);
			
			this.currTemplateReader = new InputStreamReader(indexAccessor.readObject(key));
			this.currTemplateKeyIndex++;
			
			if (this.currTemplateKeyIndex > 1) {
				return readLineBreak(cbuf, off, len);
			}
		}
		
		int result = currTemplateReader.read(cbuf, off, len);
		
		if (result != -1) {
			return result;
		}
		
		currTemplateReader.close();
		this.currTemplateReader = null;
		
		return (this.currTemplateKeyIndex < templateKeysSize) ?
			this.read(cbuf, off, len) :
			-1;
	}
	
	@Override
	public void close() throws IOException {	
		
		if (this.currTemplateReader != null) {
			currTemplateReader.close();
		}
		
		super.close();
	}
}
