package com.log10x.ext.cloud.index.query;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.ext.cloud.index.query.QueryFilterEvaluator.Constant;
import com.log10x.ext.cloud.index.shared.CompositeIndexReader;
import com.log10x.ext.cloud.index.shared.template.IndexTemplateReader;
import com.log10x.ext.edge.util.StringBuilderReader;

/**
 * A composite reader used to read stored TenxTemplates and 
 * process query search terms to be passed to matching {@link IndexQueryWriter}.
 * To learn more about how this class is instantiated by a host 10x pipeline, see: 
 * {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/query/stream.yaml# }
 */
public class IndexQueryReader extends CompositeIndexReader {

	private static final Logger logger = LogManager.getLogger(IndexQueryReader.class);

	private static final String ENRICHMENT_VALUES = "enrichmentValues";
	
	private static final Collection<String> TEXT_NAMES = Set.of("this", "text", "this.text");
	
	/**
	 * this constructor is invoked by the 10x run-time.
	 * 
	 * @param 	args
	 * 			a map of arguments passed to the 10x cli for the
	 * `		target output for which this stream is instantiated
	 * 			containing user configuration of this query
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this input
	 * 			to interact with the 10x run-time
	 */
	public IndexQueryReader(Map<String, Object> args, EvaluatorBean evaluatorBean) throws IllegalArgumentException, IOException{

		this(MapperUtil.jsonMapper(evaluatorBean).convertValue(args,
			IndexQueryOptions.class), null, evaluatorBean);
	}

	protected IndexQueryReader(IndexQueryOptions options, 
		ObjectStorageIndexAccessor indexAccessor, EvaluatorBean evaluatorBean) throws IOException {
		
		super(options, indexAccessor, evaluatorBean);

		if (options.queryFrom >= options.queryTo) {

			throw new IllegalArgumentException("'from': " + options.queryFrom + "  must be less than 'to': " + options.queryTo);
		}

		if (!options.hasFilters()) {

			throw new IllegalArgumentException("Either 'search' or 'filters' must be non-empty in " + options);
		}
	}

	private boolean queryProceesed() {
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;

		return 
			((!options.queryFilterVars.isEmpty()) ||
			 (!options.queryFilterTemplateHashes.isEmpty()));
	}
	
	@Override
	protected Reader[] createReaders(EvaluatorBean evaluatorBean) throws IOException {
				
		if (this.queryProceesed()) {
			return new Reader[0];
		}
		
		IndexQueryOptions options = (IndexQueryOptions)this.options;
		
		StringBuilderReader queryTermsReader;
		
		if ((options.querySearch != null) &&
			(!options.querySearch.isBlank())) {
			
			QueryFilterEvaluator filerEval = QueryFilterEvaluator.parse(options.querySearch);

			List<Constant> constants = filerEval.constants();

			List<String> templateTerms = new LinkedList<String>();
			List<String> eventTerms = new LinkedList<String>();

			for (Constant constant : constants) {

				String field = constant.field;
				String value = constant.value;

				if (TEXT_NAMES.contains(field)) {

					eventTerms.add(value);
					templateTerms.add(value);

				} else {

					try {

						String enrichmentEvent = MapperUtil.jsonMapper.writeValueAsString(Map.of(ENRICHMENT_VALUES, value));

						eventTerms.add(enrichmentEvent);

					} catch (JsonProcessingException e) {
						throw new UncheckedIOException(e);
					}
				}
			}

			evaluatorBean.set("querySearch", templateTerms);

			logger.warn("[TRACE-Q] createReaders: constants={}, templateTerms={}, eventTerms={}",
				constants, templateTerms, eventTerms);

			queryTermsReader = new StringBuilderReader(
				String.join(
						System.lineSeparator(),
						eventTerms)
			);

		} else {
			
			queryTermsReader = null;
		}
		
		IndexTemplateReader templateReader = new IndexTemplateReader(options, 
			this.indexAccessor, this.evaluatorBean);		

		return (queryTermsReader != null) ?
			new Reader[] {queryTermsReader, templateReader} :
			new Reader[] {templateReader};
	}
}
