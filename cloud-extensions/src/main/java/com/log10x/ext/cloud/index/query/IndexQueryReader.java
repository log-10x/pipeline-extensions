package com.log10x.ext.cloud.index.query;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;

import com.log10x.ext.cloud.index.CompositeIndexReader;
import com.log10x.ext.cloud.index.IndexTemplateReader;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.util.reader.StringBuilderReader;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * A composite reader used to read stored l1x templates and 
 * process query search terms to be passed to matching {@link IndexQueryWriter}
 */
public class IndexQueryReader extends CompositeIndexReader {

	/**
	 * this constructor is invoked by the l1x run-time.
	 * 
	 * @param 	args
	 * 			a map of arguments passed to the l1x cli for the
	 * `		target output for which this stream is instantiated
	 * 			containing user configuration of this query
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an l1x evaluator bean which allows this input
	 * 			to interact with the l1x run-time
	 */
	public IndexQueryReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws IllegalArgumentException, IOException{
				
		this(MapperUtil.jsonMapper.convertValue(args,
			IndexQueryOptions.class), null, evaluatorBean);
	}

	protected IndexQueryReader(IndexQueryOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {
		
		super(options, indexAccessor, evaluatorBean);
		
		if ((options.queryFilterFrom >= options.queryFilterTo)) {
			
			throw new IllegalStateException("'from': " + options.queryFilterFrom +
				"  must be less than 'to': " + options.queryFilterTo);
		}
	}

	private boolean queryProceesed() {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;

		return 
			((!options.queryFilterVars.isEmpty()) ||
			 (!options.queryFilterTemplateHashes.isEmpty()));
	}
	
	@Override
	protected Reader[] createReaders() throws IOException {
				
		if (this.queryProceesed()) {
			return new Reader[0];
		}
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;
		
		StringBuilderReader queryTermsReader = new StringBuilderReader(
			String.join(
				System.lineSeparator(), 
				options.queryFilterTerms
			)
		);
		
		IndexTemplateReader templateReader = new IndexTemplateReader(options, 
			this.indexAccessor, this.evaluatorBean);		

		return new Reader[] {queryTermsReader, templateReader};
	}
}
