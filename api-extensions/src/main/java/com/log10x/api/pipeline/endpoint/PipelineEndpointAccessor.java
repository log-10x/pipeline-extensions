package com.log10x.api.pipeline.endpoint;

import java.io.Closeable;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

/**
 * This interface provides an abstraction over an invocation of
 * pipeline query/scan serverless functions, over K8, AWS Lambda, Azure and GCP Cloud Functions.
 * For an AWS implementation, see: {@link https://doc.log10x.com/run/input/objectStorage/#storage-access|AWSIndexAccess}
 */
public interface PipelineEndpointAccessor extends Closeable {
	
	static HttpClient httpClient = HttpClient.newHttpClient();	
		
	/**
	 * Invoke an 10x pipeline at a remote endpoint 
	 * 
	 * @param uri		remote endpoint to execute a specified 10x pipeline
	 * @param request	arguments to pass to the 10x engine instance
	 */
	public default void invoke(URI endpointUri, String body) {
		this.invoke(httpClient, endpointUri, body);	
	}
		
	/**
	 * 
	 * Helper function for implementing {@link invoke} using {@link PipelineEndpointAccessor#httpClient}
	 * 
	 */
	public default void invoke(HttpClient client, URI endpointUri, String body) {
		
		HttpRequest httpRequest = null;
		
		try {
						
			HttpRequest.Builder requestBuilder =
					HttpRequest.newBuilder()
						.uri(endpointUri)
						.header("Content-Type", "application/json")
						.POST(BodyPublishers.ofString(body));
			
			httpRequest = requestBuilder.build();
			
			HttpResponse<String> response = client.send(httpRequest, BodyHandlers.ofString());
			
			int statusCode = response.statusCode();
			
			if (statusCode != HttpURLConnection.HTTP_OK) {
				throw new IllegalStateException("statusCode: " + statusCode);
			}
			
		} catch (Exception e) {
			throw new IllegalStateException("error posting: " + ((httpRequest != null) ? httpRequest : body), e);
		}
	}
}
