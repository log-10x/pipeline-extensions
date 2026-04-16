package com.log10x.ext.cloud.index.write;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.shared.CompositeIndexReader;
import com.log10x.ext.cloud.index.shared.template.IndexTemplateReader;
import com.log10x.ext.cloud.index.util.reader.LinebreakReader;

/**
 * This class is used to read an input object from an underlying KV storage
 * by first reading stored tenxTemplates followed by the target object bytes.
 * See: {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/index/stream.yaml}
 */
public class IndexInputReader extends CompositeIndexReader {
		
	public IndexInputReader(IndexWriteOptions indexOptions, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {
		
		super(indexOptions, indexAccessor, evaluatorBean);	
	}

	/**
	 * this constructor is invoked by the 10x run-time.
	 * 
	 * @param 	args
	 * 			a map arguments of arguments passed to the 10x cli for the
	 * `		target output for which this stream is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this output
	 * 			to interact with the 10x run-time 
	 */
	public IndexInputReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws IllegalArgumentException, IOException{
				
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexWriteOptions.class), null, evaluatorBean);
	}
	
	@Override
	protected Reader[] createReaders(EvaluatorBean evaluatorBean) throws IOException {
		
		IndexWriteOptions indexWriteOptions = (IndexWriteOptions)this.options;
		
		InputStream inputStream = this.indexAccessor.readObject(
			indexWriteOptions.indexReadObject, -1, -1);

		IndexTemplateReader templateReader = new IndexTemplateReader(this.options,
			this.indexAccessor, this.evaluatorBean);
					
		LinebreakReader byteRangeReader = new LinebreakReader(new InputStreamReader(inputStream));
		
		// makes byte ranges avail to other streams in pipeline
		evaluatorBean.set(indexWriteOptions.key(), byteRangeReader);	
		
		return new Reader[] {templateReader, byteRangeReader};
	}
}
