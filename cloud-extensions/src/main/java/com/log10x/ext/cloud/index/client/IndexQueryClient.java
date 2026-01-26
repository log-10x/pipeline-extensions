package com.log10x.ext.cloud.index.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.pipeline.endpoint.PipelineEndpointAccessor;
import com.log10x.api.pipeline.endpoint.PipelineLaunchRequest;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.util.ExecutorUtil;

/**
 * A utility class for serializing and async dispatching requests to invoke a
 * remote 10x pipeline via a REST endpoint.
 */
public class IndexQueryClient {

	private static final int N_THREADS = 16;

	private static final Logger logger = LogManager.getLogger(IndexQueryClient.class);

	private final PipelineEndpointAccessor invocationAccessor;
	private final String queryEndpoint;
	
	private final URI endpointURI;

	private final ExecutorService executorService;
	
	private final boolean debugMode;

	protected class IndexQueryClientTask implements Runnable {

		protected final PipelineLaunchRequest pipeline;
		
		protected final Object[] pipelineArgs;

		protected IndexQueryClientTask(PipelineLaunchRequest pipeline, Object[] pipelineArgs) {

			this.pipeline = pipeline;
			this.pipelineArgs = pipelineArgs;
		}

		@Override
		public void run() {

			try {

				sendRequest(this.pipeline, this.pipelineArgs);

			} catch (Exception e) {

				String req = null;

				try {

					req = MapperUtil.jsonMapper.writeValueAsString(this.pipelineArgs);

				} catch (Exception e1) {
					req = e1.toString();
				}

				logger.error("error posting request: " + req, e);
			}
		}
	}

	public IndexQueryClient(PipelineEndpointAccessor invocationAccessor, String queryEndpoint, EvaluatorBean evaluatorBean) {

		this.invocationAccessor = invocationAccessor;
		this.queryEndpoint = queryEndpoint;
		this.executorService = Executors.newFixedThreadPool(N_THREADS);
		
		try {
			this.endpointURI = new URI(this.queryEndpoint);
		} catch (URISyntaxException e) {
			throw new IllegalStateException("could not parse URI: " + queryEndpoint, e);
		}

		Object raw = evaluatorBean.env("TENX_DEBUG_QUERY_CLIENT", Boolean.FALSE);

		if (raw instanceof Boolean bool) {
			this.debugMode = bool.booleanValue();
		} else if (raw instanceof String s) {
			this.debugMode = Boolean.valueOf(s);
		} else {
			this.debugMode = false;
		}
	}

	protected void sendRequest(PipelineLaunchRequest pipeline, Object[] pipelineArgs) {

		PipelineLaunchRequest mergedRequest = null;

		try {

			mergedRequest = pipeline.overwrite(pipelineArgs);

			if (this.debugMode) {

				System.out.println(mergedRequest);

			} else {

				String body;
				
				try {
					
					body = mergedRequest.toJson();
					
				} catch (JsonProcessingException e) {
					
					throw new IllegalArgumentException("Failed creating json from " + pipelineArgs, e);
				}
						
				this.invocationAccessor.invoke(this.endpointURI, body);
			}

		} catch (Exception e) {

			throw new IllegalStateException("error invoking with: " + mergedRequest, e);
		}
	}
	
	public void send(PipelineLaunchRequest pipeline, Object... pipelineArgs) {

		if (logger.isDebugEnabled()) {

			try {

				logger.debug(
						"submitting request: " + MapperUtil.jsonMapper.writeValueAsString(pipelineArgs));

			} catch (JsonProcessingException e) {

				logger.error(e);
			}
		}

		executorService.submit(new IndexQueryClientTask(pipeline, pipelineArgs));
	}

	@Override
	public String toString() {
		
		return String.format("IndexQueryClient(%s)", this.queryEndpoint); 
	}

	public void close() {
		
		ExecutorUtil.safeAwaitTermination(this.executorService);
	}
}
