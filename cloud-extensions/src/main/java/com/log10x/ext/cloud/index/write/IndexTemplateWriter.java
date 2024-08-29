package com.log10x.ext.cloud.index.write;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.log10x.ext.cloud.index.BaseIndexWriter;
import com.log10x.ext.cloud.index.interfaces.IndexContainerOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * This class is used to write the l1x template values of events read
 * from a target object residing within a KV storage (e.g. AWS S3) to
 * a KV storage container (e.g. bucket)
 * To see how this class is wired into an l1x pipeline,
 * see: {@link https://github.com/l1x-co/config/blob/main/pipelines/run/modules/input/objectStorage/index/stream.yaml}
 */
public class IndexTemplateWriter extends BaseIndexWriter {
		
	public static final String TEMPLATE_HASH_PREFIX = "templateHash:{";

	public static final String TEMPLATE_HASH_POSTFIX = "}";
	
	private final String templateInlinePath;

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
	public IndexTemplateWriter(Map<String, Object> args, EvaluatorBean evaluatorBean){
			
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexWriteOptions.class), null, evaluatorBean);
	}

	public IndexTemplateWriter(IndexContainerOptions indexInputOptions, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) {
		
		super(indexInputOptions, indexAccessor, evaluatorBean);
		
		this.templateInlinePath = this.indexAccessor.templateInlinePath(
			indexInputOptions.prefix());
	}

	@Override
	public synchronized void flush() throws IOException {
		
		String templatePath;
		String templateName;
		String templateValue;

		String key = this.templateInlinePath + currChars.builder;
				
		if (indexAccessor.keyByteLength(key) != -1) {
			
			templatePath = this.templateInlinePath;
			templateName = currChars.builder.toString();
			templateValue = null;		
					
		} else {
			
			int hashStart = currChars.builder.indexOf(TEMPLATE_HASH_PREFIX);
			int hashEnd = currChars.builder.indexOf(TEMPLATE_HASH_POSTFIX, hashStart);
			
			if ((hashStart == -1) || 
				(hashEnd == -1)) {
				
				throw new IllegalStateException("could not extract " + 
					TEMPLATE_HASH_PREFIX + " from: " + this.currChars);
			}
			
			String templateHash = currChars.builder.substring(hashStart, hashEnd);
						
			templatePath = indexAccessor.templateEnclosedPath(options.prefix());
			
			templateName = templateHash;	
			templateValue = currChars.builder.toString();
		}
		
		indexAccessor.putIndexObject(templatePath, templateName,
			templateValue, Collections.emptyMap());	
		
		super.flush();
	}
}
