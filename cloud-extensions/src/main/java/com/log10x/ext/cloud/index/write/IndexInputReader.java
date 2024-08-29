package com.log10x.ext.cloud.index.write;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import com.log10x.ext.cloud.index.CompositeIndexReader;
import com.log10x.ext.cloud.index.IndexTemplateReader;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.util.reader.LinebreakReader;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * This class is used to read an input object from an underlying KV storage
 * by first reading an stored l1xTemplates followed by the target object bytes.
 */
public class IndexInputReader extends CompositeIndexReader {
		
	public IndexInputReader(IndexWriteOptions indexOptions, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {
		
		super(indexOptions, indexAccessor, evaluatorBean);	
	}

	/**
	 * this constructor is invoked by the l1x run-time.
	 * 
	 * @param 	args
	 * 			a map arguments of arguments passed to the l1x cli for the
	 * `		target output for which this stream is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an l1x evaluator bean which allows this output
	 * 			to interact with the l1x run-time 
	 */
	public IndexInputReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws IllegalArgumentException, IOException{
				
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexWriteOptions.class), null, evaluatorBean);
	}
	
	@Override
	protected Reader[] createReaders() throws IOException {
		
		IndexWriteOptions indexWriteOptions = (IndexWriteOptions)this.options;
		
		InputStream inputStream = this.indexAccessor.readObject(
			indexWriteOptions.indexReadObject, -1, -1);

		IndexTemplateReader templateReader = new IndexTemplateReader(this.options,
			this.indexAccessor, this.evaluatorBean);
					
		LinebreakReader byteRangeReader = new LinebreakReader(new InputStreamReader(inputStream));
		
		// makes byte ranges avail to other streams in pipeline
		evaluatorBean.dict(indexWriteOptions.key(), byteRangeReader);	
		
		return new Reader[] {templateReader, byteRangeReader};
	}
}
