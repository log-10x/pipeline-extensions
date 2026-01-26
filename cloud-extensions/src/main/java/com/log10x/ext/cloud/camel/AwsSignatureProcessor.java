package com.log10x.ext.cloud.camel;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.sample.auth.AWS4SignerBase;
import com.amazonaws.services.s3.sample.auth.AWS4SignerForAuthorizationHeader;
import com.amazonaws.services.s3.sample.auth.AWSConsts;
import com.amazonaws.services.s3.sample.util.BinaryUtils;

import io.netty.handler.codec.http.HttpHeaderNames;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

/**
 * Camel route processor used to sign AWS REST requests
 * See: {@link https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_aws-signing.html}
 */
public class AwsSignatureProcessor extends BaseRouteProcessor {

	private static final Logger logger = LogManager.getLogger(AwsSignatureProcessor.class);

	public AwsSignatureProcessor() {
	}

	@Override
	public void process(Exchange exchange) throws Exception {

		String method = routeStrProp(exchange, AWSConsts.HTTP_METHOD);

		String contentType = routeStrProp(exchange, HttpHeaderNames.CONTENT_TYPE.toString());

		String service = routeStrProp(exchange, AWSConsts.AWS_SERVICE_PROP);

		String accessKey = routeStrProp(exchange, AWSConsts.AWS_ACCESS_KEY_ID_PROP);

		String secretKey = routeStrProp(exchange, AWSConsts.AWS_SECRET_KEY_PROP);

		String regionName = routeStrProp(exchange, AWSConsts.AWS_REGION_PROP);

		String request = routeStrProp(exchange, AWSConsts.AWS_REQUEST_PROP);

		String host = routeStrProp(exchange, AWSConsts.AWS_HOST_PROP);

		String target = routeStrProp(exchange, AWSConsts.AWS_TARGET_HEADER);

		// Session token for assumed roles/SSO (may be null)
		String sessionToken = null;

		// Fallback to AWS credential chain if credentials not explicitly provided
		if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey)) {

			logger.info("AWS credentials not explicitly provided, using default credential chain");

			try (DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build()) {

				AwsCredentials credentials = credentialsProvider.resolveCredentials();
				accessKey = credentials.accessKeyId();
				secretKey = credentials.secretAccessKey();

				// Handle session tokens for assumed roles, SSO, etc.
				if (credentials instanceof AwsSessionCredentials sessionCreds) {
					sessionToken = sessionCreds.sessionToken();
					logger.debug("Using session credentials with token");
				}

				logger.info("Resolved AWS credentials from default provider chain (accessKeyId: {}...)",
					accessKey.substring(0, Math.min(8, accessKey.length())));

			} catch (Exception e) {
				throw new RuntimeException(
					"No AWS credentials provided and default credential chain failed: " + e.getMessage(), e);
			}
		}

		// Fallback to AWS region chain if region not explicitly provided
		if (isNullOrEmpty(regionName)) {
			try {
				regionName = DefaultAwsRegionProviderChain.builder().build().getRegion().id();
				logger.info("AWS region not explicitly provided, resolved from default chain: {}", regionName);

				// Update exchange properties so subsequent route steps use the resolved region
				exchange.setProperty(AWSConsts.AWS_REGION_PROP, regionName);

				// Rebuild the AWS host with the resolved region (format: service.region.amazonaws.com)
				if (service != null && !service.isEmpty()) {
					host = service + "." + regionName + ".amazonaws.com";
					exchange.setProperty(AWSConsts.AWS_HOST_PROP, host);
					logger.info("Rebuilt AWS host with resolved region: {}", host);
				}
			} catch (Exception e) {
				throw new RuntimeException(
					"No AWS region provided and default region chain failed: " + e.getMessage(), e);
			}
		}

		String endpointUrlString = "https://" + host;

		URL endpointUrl;

		try {

			endpointUrl = new URI(endpointUrlString).toURL();

		} catch (MalformedURLException e) {
			throw new RuntimeException("Unable to parse service endpoint: " + e.getMessage());
		}

		// precompute hash of the body content
		byte[] contentHash = AWS4SignerBase.hash(request);
		String contentHashString = BinaryUtils.toHex(contentHash);

		Map<String, String> headers = new HashMap<String, String>();

		headers.put(AWSConsts.AWS_CONTENT_SHA_HEADER, contentHashString);
		headers.put(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(request.length()));
		headers.put(AWSConsts.AWS_TARGET_HEADER, target);
		headers.put(HttpHeaderNames.CONTENT_TYPE.toString(), contentType);

		// Add session token header if using temporary credentials
		if (sessionToken != null) {
			headers.put(AWSConsts.AWS_SECURITY_TOKEN, sessionToken);
		}

		AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(endpointUrl,
			method, service, regionName);

		String authorization = signer.computeSignature(headers, null, // no query parameters
				contentHashString, accessKey, secretKey);

		// express authorization for this as a header
		headers.put(AWSConsts.AUTHORIZATION_HEADER, authorization);

		Message message = exchange.getMessage();

		for (Map.Entry<String, String> entry : headers.entrySet()) {
			message.setHeader(entry.getKey(), entry.getValue());
		}

		if (logger.isDebugEnabled()) {

			logger.debug("signed AWS request for body: " +
				request + ", headers: " + headers);
		}
	}

	private static boolean isNullOrEmpty(String str) {
		return str == null || str.isEmpty();
	}
}
