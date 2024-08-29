package com.log10x.ext.cloud.micrometer.registry;

import java.io.IOException;
import java.util.Map;

import com.log10x.ext.edge.micrometer.MapRegistryConfig;
import com.log10x.ext.edge.micrometer.MetricRegistryFactory;

import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

/**
 * An implementation of the {@link MetricRegistryFactory} interface for AWS CloudWatch metrics
 */
public class CloudWatchMetricRegistryFactory implements MetricRegistryFactory {
	
	public static final String AWS_ACCESS_KEY_ID = "awsAccessKeyId";
	
	public static final String AWS_SECRET_KEY = "awsSecretKey";
	
	protected static class CloudWatchConfigRegistry extends MapRegistryConfig 
		implements io.micrometer.cloudwatch2.CloudWatchConfig {

		public CloudWatchConfigRegistry(Map<String, Object> options) {
			super(options);
		}
		
		public CloudWatchAsyncClient createClient() {
			
			Object secretKey = this.get(AWS_SECRET_KEY, true);		
			Object accessKey = this.get(AWS_ACCESS_KEY_ID, true);
			
			return CloudWatchAsyncClient.builder().credentialsProvider(
				
					new AwsCredentialsProvider() {
					
					@Override
					public AwsCredentials resolveCredentials() {
						
						return AwsBasicCredentials.create( 
							accessKey.toString(), 
							secretKey.toString()
						);
					}
				}
					
			).build();		
		}
	}
			
	@Override
	public CloudWatchMeterRegistry create(Map<String, Object> options) throws IOException {
			
		CloudWatchConfigRegistry config = new CloudWatchConfigRegistry(options);
		
		return new CloudWatchMeterRegistry(config, Clock.SYSTEM, 
			config.createClient());	
	}
}
