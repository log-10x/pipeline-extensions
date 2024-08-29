package com.log10x.ext.cloud.index.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.ext.cloud.index.interfaces.PipelineInvocationAccessor;
import com.log10x.ext.cloud.index.util.ExecutorUtil;
import com.log10x.ext.edge.invoke.PipelineLaunchRequest;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * A utility class for serializing and async dispatching requests to invoke a
 * remote l1x pipeline via a REST endpoint.
 */
public class IndexQueryClient {

	private static final boolean DEBUG = false;

	private static final int N_THREADS = 16;

	private static final Logger logger = LogManager.getLogger(IndexQueryClient.class);

	private final PipelineInvocationAccessor invocationAccessor;
	private final String queryEndpoint;
	
	private final URI endpointURI;

	private final ExecutorService executorService;

	protected class IndexQueryClientTask implements Runnable {

		protected final Object values;

		protected final Object pipelineArgs;

		protected final PipelineLaunchRequest pipeline;

		protected IndexQueryClientTask(Object values, Object pipelineArgs, PipelineLaunchRequest pipeline) {

			this.values = values;
			this.pipelineArgs = pipelineArgs;
			this.pipeline = pipeline;
		}

		@Override
		public void run() {

			try {

				sendRequest(this.pipeline, this.values, this.pipelineArgs);

			} catch (Exception e) {

				String req = null;

				try {

					req = MapperUtil.jsonMapper.writeValueAsString(this.values);

				} catch (Exception e1) {
					req = e1.toString();
				}

				logger.error("error posting request: " + req, e);
			}
		}
	}

	public IndexQueryClient(PipelineInvocationAccessor invocationAccessor, String queryEndpoint) {

		this.invocationAccessor = invocationAccessor;
		this.queryEndpoint = queryEndpoint;
		this.executorService = Executors.newFixedThreadPool(N_THREADS);
		
		try {
			this.endpointURI = new URI(this.queryEndpoint);
		} catch (URISyntaxException e) {
			throw new IllegalStateException("could not parse URI: " + queryEndpoint, e);
		}
	}

	protected void sendRequest(PipelineLaunchRequest pipeline, Object request, Object pipelineArgs) {

		PipelineLaunchRequest mergedRequest = null;

		try {

			mergedRequest = pipeline.override(request, pipelineArgs);

			if (DEBUG) {

				System.out.println(mergedRequest);

			} else {

				String body;
				
				try {
					
					body = mergedRequest.toJson();
					
				} catch (JsonProcessingException e) {
					
					throw new IllegalArgumentException("Failed creating json from " + request, e);
				}
						
				this.invocationAccessor.invoke(this.endpointURI, body);
			}

		} catch (Exception e) {

			throw new IllegalStateException("error invoking with: " + mergedRequest, e);
		}
	}

	public void send(Collection<?> requests, Object pipelineArgs, PipelineLaunchRequest pipeline) {

		for (Object request : requests) {
			
			send(request, pipelineArgs, pipeline);
		}
	}
	
	public void send(Object request, Object pipelineArgs, PipelineLaunchRequest pipeline) {

		if (logger.isDebugEnabled()) {

			try {

				logger.debug(
						"submitting request: " + MapperUtil.jsonMapper.writeValueAsString(request));

			} catch (JsonProcessingException e) {

				logger.error(e);
			}
		}

		executorService.submit(new IndexQueryClientTask(request, pipelineArgs, pipeline));
	}

	@Override
	public String toString() {
		
		return String.format("IndexQueryClient(%s)", this.queryEndpoint); 
	}

	public void close() {
		
		ExecutorUtil.safeAwaitTermination(this.executorService);
	}
}
