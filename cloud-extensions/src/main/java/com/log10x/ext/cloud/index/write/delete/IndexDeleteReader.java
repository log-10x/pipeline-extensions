package com.log10x.ext.cloud.index.write.delete;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;
import com.log10x.ext.cloud.index.shared.BaseIndexReader;
import com.log10x.ext.cloud.index.util.StringListReader;
import com.log10x.ext.cloud.index.write.TargetObjectReverseIndex;

public class IndexDeleteReader extends BaseIndexReader {

	private static final Logger logger = LogManager.getLogger(IndexDeleteReader.class);

	private final TargetObjectReverseIndex reverseIndex;
	
	private final StringListReader stringListReader;
	
	/**
	 * this constructor is invoked by the 10x engine.
	 * 
	 * @param 	args
	 * 			the values of the 'IndexDelete' option group
	 * 		    instance for which the IndexDeleteWriter is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this input
	 * 			to interact with the 10x run-time 
	 * @throws IllegalArgumentException 
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException 
	 * @throws DatabindException 
	 * @throws StreamReadException 
	 */
	public IndexDeleteReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException, IllegalArgumentException, StreamReadException, DatabindException, IOException{
		
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexDeleteOptions.class), null, evaluatorBean);
	}
	
	protected IndexDeleteReader(IndexDeleteOptions options, 
		ObjectStorageIndexAccessor indexAccessor,
		EvaluatorBean evaluatorBean) throws StreamReadException, DatabindException, IOException {
		
		super(options, indexAccessor, evaluatorBean);
		
		String path = this.indexAccessor.indexObjectPath(IndexObjectType.reverseIndex,
			options.target());
		
		String key = path + this.indexAccessor.keyPathSeperator() + 
			options.indexDeleteObject;
		
		InputStream inputStream = this.indexAccessor.readObject(key);
				
		this.reverseIndex = MapperUtil.jsonMapper.readValue(inputStream, 
			TargetObjectReverseIndex.class);
		
		this.stringListReader = new StringListReader(reverseIndex.indexObjects);
	}
	
	@Override
	public synchronized void close() throws IOException {
		
		IndexDeleteOptions options = (IndexDeleteOptions)this.options;
				
		int deleted = indexAccessor.deleteObjects(reverseIndex.indexObjects);
		
		if (logger.isDebugEnabled()) {
			
			logger.debug("deleted index objects" +
			 ". target: " + options.indexDeleteObject +
			 ", deleted:" + deleted + "/" + reverseIndex.indexObjects.size());		
		}
		
		super.close();
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		return stringListReader.read(cbuf, off, len);
	}
}
