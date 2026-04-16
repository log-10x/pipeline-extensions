package com.log10x.ext.cloud.index.write;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.FileUtil;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;
import com.log10x.ext.cloud.index.interfaces.options.IndexContainerOptions;
import com.log10x.ext.cloud.index.shared.BaseIndexWriter;
import com.log10x.ext.cloud.index.shared.template.IndexTemplates;

/**
 * This class is used to write the tenxTemplate values of events read
 * from a target object residing within a KV storage (e.g. AWS S3) to
 * a KV storage container (e.g. bucket)
 * To see how this class is wired into an 10x pipeline,
 * see: {@link https://github.com/log-10x/config/blob/main/pipelines/run/modules/input/objectStorage/index/stream.yaml}
 */
public class IndexTemplateWriter extends BaseIndexWriter {
			
	private static final Logger logger = LogManager.getLogger(IndexTemplateWriter.class);

	private final File outputFile; 
	
	private final OutputStream outputStream;
		
	private final MessageDigest messageDigest;
	
	private long outputLength;
	
	private int templateSize;

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
	 * @throws IllegalArgumentException 
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException 
	 */
	public IndexTemplateWriter(Map<String, Object> args, EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException, IllegalArgumentException, IOException{
			
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexWriteOptions.class), null, evaluatorBean);
	}

	@SuppressWarnings("resource")
	public IndexTemplateWriter(IndexContainerOptions indexInputOptions, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException, IOException {
		
		super(indexInputOptions, indexAccessor, evaluatorBean);
		
		this.messageDigest = MessageDigest.getInstance("MD5");
		
		IndexWriteOptions options = (IndexWriteOptions)this.options;
		
		this.outputFile = File.createTempFile(options.key(), null);
		this.outputStream = new BufferedOutputStream(new FileOutputStream(this.outputFile));	
	}

	@Override
	public synchronized void flush() throws IOException {
			
		try {
			
			if (this.templateSize > 0) {
				
				outputStream.write(LINE_BREAK_BYTES);
				this.outputLength += LINE_BREAK_BYTES.length;				
			}
			
			byte[] bytes = this.currChars.builder.toString().getBytes();
			
			this.outputLength += bytes.length;
					
			messageDigest.update(bytes);	
			outputStream.write(bytes);
						
			this.templateSize++;
			
		} finally {		
			
			super.flush();
		}
	}
	
	private void uploadTemplateFile() throws IOException {
		
		IndexWriteOptions options = (IndexWriteOptions) this.options;

		String path = indexAccessor.indexObjectPath(IndexObjectType.template, options.target());

		String key = IndexTemplates.templateFileKey(this.messageDigest, false);

		InputStream inputStream = null;
		
		try {
	
			FileUtil.flush(outputStream);
			FileUtil.close(outputStream);

			if (this.outputLength > 0) {
			
				inputStream = new BufferedInputStream(new FileInputStream(this.outputFile));
			
				indexAccessor.putObject(path, key, inputStream,
					this.outputLength, Collections.emptyMap());
			}
		
		} catch (Exception e) {
			
			throw new IllegalStateException("error uploading: " + 
				this.outputFile + " to: " + path + indexAccessor.keyPathSeperator() + key, e);
			
		} finally {
						
			if (inputStream != null) {
				inputStream.close();
			}
		}
						
		if (logger.isDebugEnabled()) {	
			
			logger.debug("uploaded template file" +
				". file: " + this.outputFile +
				", bytes: " + this.outputLength +
				", templates: " + this.templateSize
			);
		}
	}
			
	@Override
	public synchronized void close() throws IOException {

		try {

			this.uploadTemplateFile();

			if (this.shouldMergeTemplates()) {

				IndexTemplates indexTemplates = IndexTemplates.scan(this.indexAccessor, options.target(), false);

				indexTemplates.mergeTemplateFiles();
			}

		} finally {

			if (!outputFile.delete()) {
				logger.warn("could not delete: " + this.outputFile);
			}

			super.close();
		}
	}
	
	private boolean shouldMergeTemplates() throws IOException {
		
		IndexWriteOptions options = (IndexWriteOptions)this.options;
				
		if (options.indexWriteTemplateMergeInterval == 0) {		
			return true;	
		}
			
		IndexTemplates mergedTemplates = IndexTemplates.scan(this.indexAccessor,
			options.target(), true);
		
		long lastMergeEpoch = mergedTemplates.lastMergeEpoch();
		
		if (lastMergeEpoch == -1)  {
			return true;
		}
		
		long now = System.currentTimeMillis();
		
		return (now - lastMergeEpoch > options.indexWriteTemplateMergeInterval);
	}
}
