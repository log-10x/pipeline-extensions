package com.log10x.ext.cloud.index.access;

import static com.amazonaws.services.s3.sample.auth.AWSConsts.AUTHORIZATION_HEADER;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_CONTENT_SHA_HEADER;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_HOST;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_REGION_ENV;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_SECURITY_TOKEN;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_TARGET_HEADER;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.CODE;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.CONTENTS;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.CONT_TOKEN_PARAM;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.HTTP_GET_METHOD;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.IS_TRUNC;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.LIST_KEY;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.LIST_TYPE_PARAM;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.LIST_TYPE_PARAM_VAL;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.MESSAGE;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.NEXT_CONT_TOKEN;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.NO_SUCH_KEY;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.PREFIX_PARAM;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.S3_SERVICE;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.START_AFTER_PARAM;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.unbescape.xml.XmlEscape;

import com.amazonaws.services.s3.sample.auth.AWS4SignerBase;
import com.amazonaws.services.s3.sample.auth.AWS4SignerForAuthorizationHeader;
import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.pipeline.launch.PipelineLaunchOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.options.ObjectStorageAccessOptions;
import com.log10x.ext.cloud.index.util.stream.LazyInputStream;
import com.log10x.ext.cloud.index.util.utf.Utf8;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.BlockingInputStreamAsyncRequestBody;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Implementation of the {@link ObjectStorageIndexAccessor} interface over AWS services,
 * utilizing S3 buckets and Lambda functions.
 *
 * Similar implementations can be added for other cloud solutions as Azure and GCP.
 */
public class AWSIndexAccess implements ObjectStorageIndexAccessor {

	public static AWSClientsCache CLIENTS_CACHE = new AWSClientsRefCountCache();
	
	/**
	 * Enum defining the endpoint type for remote invocations.
	 */
	public enum EndpointType {
		LAMBDA,
		SQS,
		HTTP
	}

	/**
	 * When calling {@link #invoke} with a 'url' beginning with this prefix, will assume
	 * the rest of the 'url' is the name of an AWS Lambda which is the invocation target.
	 *
	 * Otherwise, will assume the 'url' is the invocation target.
	 */
	public static final String ENDPOINT_TYPE_KEY = "invoke";
	
	public static final String ENDPOINT_TYPE_VALUE = "lambda";
	
	private static final String SEPARATOR = String.valueOf('/');

	private static final boolean USE_LAZY_STREAM = true;
	private static final boolean LAZY_STREAM_COPY_TO_BAIS = false;

	/**
	 * Threshold for triggering cleanup of completed async requests.
	 * When the queue size exceeds this value, completed futures are removed.
	 */
	private static final int ASYNC_CLEANUP_THRESHOLD = 100;
	
	protected final ObjectStorageAccessOptions options;

	private static final Logger logger = LogManager.getLogger(AWSIndexAccess.class);
	
	public static final int MAX_S3_KEY_BYTES = 1024;
	
	public static final int MAX_S3_DEL_OBJECTS = 1000;

	private static final String XML_OPEN = "<";
	private static final String XML_CLOSE = ">";
	
	private static final String RESPONSE_CODE_OPEN = xmlNode(CODE, false);
	private static final String RESPONSE_CODE_CLOSE = xmlNode(CODE, true);
	
	private static final String RESPONSE_MESSAGE_OPEN = xmlNode(MESSAGE, false);
	private static final String RESPONSE_MESSAGE_CLOSE = xmlNode(MESSAGE, true);
	
	private static final String NEXT_CONT_TOKEN_OPEN = xmlNode(NEXT_CONT_TOKEN, false);
	private static final String NEXT_CONT_TOKEN_CLOSE = xmlNode(NEXT_CONT_TOKEN, true);

	private static final String IS_TRUNC_OPEN = xmlNode(IS_TRUNC, false);
	private static final String IS_TRUNC_CLOSE = xmlNode(IS_TRUNC, true);

	private static final String LIST_KEY_OPEN = xmlNode(CONTENTS, false) + xmlNode(LIST_KEY, false) ;
	private static final String LIST_KEY_CLOSE = xmlNode(LIST_KEY, true) + XML_OPEN; // Any tag after us is ok

	private static final char ESC_TARGET = 0;
	private static final char ESC_TO = 127;
	private static final char ESC_CHAR = '\\';
	
	private static final int MAX_AWS_TAG_LEN = 200;
	
	protected class S3IndexIterator implements Iterator<List<String>> {

		private final String prefix;

		private final String searchAfter;

		private final boolean failOnMissing;

		private ListObjectsResponseOutput objects;

		private int requests;

		protected S3IndexIterator(String prefix, String searchAfter, boolean failOnMissing) {
			this.searchAfter = searchAfter;
			this.prefix = prefix;
			this.failOnMissing = failOnMissing;
		}
			
		private void listObjects() {
			
			try {
				
				this.objects = iterateIndexObjects(
						this.prefix,
						this.searchAfter,
						(this.objects != null ? objects.contToken : null),
						this.failOnMissing);
		
			} catch (Exception e) {
				
				throw new IllegalStateException("error iterating prefix: " + prefix +
					", searchAfter: " + searchAfter, e);
			}
								
			this.requests++;
		}
					
		@Override
		public boolean hasNext() {
			
			if (this.objects != null) {
				
				if ((this.objects.keys.isEmpty()) ||
					(this.objects.contToken == null)) {

					return false;
				}
			}
			
			this.listObjects();

			return !objects.keys.isEmpty();	
		}

		@Override
		public List<String> next() {
			return this.objects.keys;
		}
		
		@Override
		public String toString() {
			
			return String.format("contToken: %s requests: %d" ,
				(this.objects != null) ? objects.contToken : null,
				this.requests);
		}
	}

	protected static class ListObjectsResponseOutput {

		protected final List<String> keys;

		protected final String contToken;

		protected final boolean isTrunc;

		protected ListObjectsResponseOutput(List<String> keys, String contToken, boolean isTrunc) {

			this.keys = keys;
			this.contToken = contToken;
			this.isTrunc = isTrunc;
		}
	}
	
	private final String regionName;

	private final String uuid;

	private final AWSClients clients;

	private final ConcurrentLinkedQueue<CompletableFuture<?>> inFlightAsyncRequests;

	private final AtomicInteger requestCounter;

	private final EndpointType endpointType;

	private final QueryEventLog queryEventLog;

	private final String indexBucketName;
	private final String indexBucketPath;
	private final String cloudwatchLogGroup;

	// this constructor is invoked via reflection
	public AWSIndexAccess(ObjectStorageAccessOptions options, EvaluatorBean evaluatorBean) {

		this.options = options;

		this.regionName = evaluatorBean.env(AWS_REGION_ENV, true).toString();

		this.uuid = (String) evaluatorBean.env(PipelineLaunchOptions.UNIQUE_ID);

		this.clients = CLIENTS_CACHE.get(this.uuid);

		this.inFlightAsyncRequests = new ConcurrentLinkedQueue<>();
		this.requestCounter = new AtomicInteger(0);

		this.endpointType = parseEndpointType(options.args().get(ENDPOINT_TYPE_KEY));

		this.queryEventLog = this.clients.queryEventLog();
		
		String indexContainer = options.indexContainer();
		int index = indexContainer.indexOf(SEPARATOR);
		
		String path;
		
		if (index == -1) {
			this.indexBucketName = indexContainer;
			path = "";
		} else {
			this.indexBucketName = indexContainer.substring(0, index);
			path = indexContainer.substring(index + 1, indexContainer.length());
		}
		
		if ((!path.isBlank()) &&
			(!path.endsWith(SEPARATOR))) {
			
			path += SEPARATOR;
		}
		
		this.indexBucketPath = path;
		
		this.cloudwatchLogGroup = options.queryLogGroup();
	}

	private static EndpointType parseEndpointType(String typeValue) {

		if ((typeValue == null) ||
			(typeValue.isEmpty())) {

			return EndpointType.HTTP;
		}

		String lowerCaseType = typeValue.toLowerCase();

		for (EndpointType type : EndpointType.values()) {
			if (type.name().toLowerCase().equals(lowerCaseType)) {
				return type;
			}
		}

		throw new IllegalArgumentException("Invalid endpoint type: " + typeValue + ". Valid values are: " + Arrays.toString(EndpointType.values()));
	}

	/**
	 * Removes completed futures from the queue to prevent unbounded memory growth.
	 * This method is thread-safe and can be called concurrently with additions.
	 */
	private void cleanupCompletedRequests() {
		int initialSize = this.inFlightAsyncRequests.size();

		// removeIf is atomic per element and thread-safe with ConcurrentLinkedQueue
		boolean removed = this.inFlightAsyncRequests.removeIf(future -> {
			return future.isDone();
		});

		if ((removed) &&
			(logger.isDebugEnabled())) {

			int finalSize = this.inFlightAsyncRequests.size();
			int removedCount = initialSize - finalSize;
			logger.debug("Cleaned up {} completed async requests. Remaining: {}", removedCount, finalSize);
		}
	}

	/**
	 * Adds a future to the tracking queue and triggers cleanup if threshold is
	 * reached. This method is thread-safe.
	 */
	private void trackAsyncRequest(CompletableFuture<?> future) {
		this.inFlightAsyncRequests.add(future);

		// Periodically trigger cleanup based on request count
		int count = this.requestCounter.incrementAndGet();

		if (count % ASYNC_CLEANUP_THRESHOLD == 0) {
			cleanupCompletedRequests();
		}
	}

	private static String xmlNode(String name, boolean close) {
		return XML_OPEN + (close ? "/" : "") + name + XML_CLOSE;
	}
	
	private static CharSequence unescapeKey(CharSequence key) {
		
		int len = key.length();
		
		int from = -1;
	
		for (int i = 0; i < len; i++) {
			
			char c = key.charAt(i);
			
			if (c == ESC_TO) {
				
				from = i;
				break;
			}
		}
		
		if (from == -1) {
			return key;
		}
		
		for (int i = 0; i < from; i++) {
			char c = key.charAt(i);
			
			if (c == ESC_CHAR) {
				
				from = i;
				break;
			}
		}
		
		StringBuilder result = new StringBuilder().append(key, 0, from);
				
		int index = from;
		
		while (index < len) {
					
			char c = key.charAt(index);
			
			switch (c) {
			
			case ESC_CHAR:		
				
				if (index < len - 1) {
					
					char n = key.charAt(index + 1);
					
					if (n == ESC_CHAR) {
						result.append(ESC_CHAR);
						index += 2;

						continue;
					}

					if (n == ESC_TO) {
						
						result.append(ESC_TO);
						index += 2;
						
						continue;
					}
					
				}
				
				// Should not be here.. 
				result.append(c);
				index++;	
				
				continue;
				
			case ESC_TO:		
				result.append(ESC_TARGET);
				index++;
				continue;
		
			default:
				result.append(c);
				index++;					
			}			
		}
		
		return result;	
	}

	private static CharSequence escapeKey(CharSequence s) {
		
		int len = s.length();
		
		int ctrlIndex = -1;
	
		for (int i = 0; i < len; i++) {
			
			char c = s.charAt(i);
			
			if ((c == ESC_TARGET) ||
				(c == ESC_TO)) {

				ctrlIndex = i;
				break;
			}
		}
		
		if (ctrlIndex == -1) {
			return s;
		}
		
		for (int i = 0; i < ctrlIndex; i++) {
			
			char c = s.charAt(i);
			
			if (c == ESC_CHAR) {
				
				ctrlIndex = i;
				break;
			}
		}
		
		StringBuilder result = new StringBuilder().
			append(s, 0, ctrlIndex);
		
		for (int i = ctrlIndex; i < len; i++) {
			
			char c = s.charAt(i);
			
			switch (c) {

				case ESC_CHAR:
					result.append(ESC_CHAR).append(ESC_CHAR);
					continue;
			
				case ESC_TO:		
					
					result.append(ESC_CHAR).append(ESC_TO);
					continue;
			
				case ESC_TARGET:	
					
					result.append(ESC_TO);
					continue;	
				
				default:
					result.append(c);			
			}			
		}
		
		return result;	
	}
	
	/**
	 * Environment variable for custom AWS endpoint URL (e.g., LocalStack, MinIO).
	 * When set, all S3 operations will use this endpoint instead of the default AWS endpoint.
	 */
	private static final String AWS_ENDPOINT_URL_ENV = "AWS_ENDPOINT_URL";

	private String regionURLName() {

		return regionName.equals("us-east-1") ?
			"" :
			"-" + this.regionName;
	}

	/**
	 * Returns the custom endpoint URL if configured, null otherwise.
	 */
	private static String getCustomEndpointUrl() {
		return System.getenv(AWS_ENDPOINT_URL_ENV);
	}

	private URL listObjectsURL() {

		String customEndpoint = getCustomEndpointUrl();

		StringBuilder uriBuilder = new StringBuilder();

		if ((customEndpoint != null) &&
			(!customEndpoint.isBlank())) {

			// Use custom endpoint (LocalStack, MinIO, etc.)
			// Custom endpoints typically use path-style access: endpoint/bucket
			uriBuilder.append(customEndpoint);
			if (!customEndpoint.endsWith("/")) {
				uriBuilder.append("/");
			}
			uriBuilder.append(this.indexBucketName);

		} else {

			// Use standard AWS S3 endpoint
			uriBuilder.
				append("https://").
				append(S3_SERVICE).
				append(this.regionURLName()).
				append(".").
				append(AWS_HOST).
				append("/").
				append(this.indexBucketName);
		}

		try {

			return new URI(uriBuilder.toString()).toURL();

		} catch (Exception e) {
			throw new RuntimeException("Unable to parse service endpoint: " + uriBuilder, e);
		}
	}

	protected URI addQueryParams(URI base, Map<String, String> queryParams) throws URISyntaxException {
		
		if ((queryParams == null) ||
			(queryParams.isEmpty())) {
			
			return base;
		}
		
		URIBuilder builder = new URIBuilder(base);

		for (Map.Entry<String, String> entry : queryParams.entrySet()) {
			builder.addParameter(entry.getKey(), entry.getValue());
		}

		return builder.build();
	}
	
	protected ListObjectsResponseOutput iterateIndexObjects(
		String prefix, String startAfter, String contToken, boolean failOnMissing) throws IOException, URISyntaxException, InterruptedException {

		URL baseEndpointUrl = this.listObjectsURL();

		// Normalize prefix to ensure it ends with '/' for proper folder behavior
		// This prevents "foo/bar" from matching both "foo/bar/file" and "foo/barfile"
		String normalizedPrefix = this.indexBucketPath;

		if (prefix != null) {
			normalizedPrefix += (prefix.endsWith(SEPARATOR) ? prefix : prefix + SEPARATOR);
		}

		// Resolve credentials from the provider (supports full AWS credentials chain)
		AwsCredentials credentials = this.clients.credentialsProvider().resolveCredentials();
		String accessKey = credentials.accessKeyId();
		String secretKey = credentials.secretAccessKey();
		String sessionToken = null;

		// Check if we have session credentials (temporary credentials from STS, IRSA, etc.)
		if (credentials instanceof AwsSessionCredentialsIdentity) {
			sessionToken = ((AwsSessionCredentialsIdentity) credentials).sessionToken();
		}

		Map<String, String> headers = new HashMap<String, String>(4);

		headers.put(AWS_CONTENT_SHA_HEADER, AWS4SignerBase.EMPTY_BODY_SHA256);
		headers.put(AWS_TARGET_HEADER, this.indexBucketName);

		AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
			baseEndpointUrl, HTTP_GET_METHOD, S3_SERVICE, regionName);

		Map<String, String> queryParameters = new LinkedHashMap<>(4);

		queryParameters.put(LIST_TYPE_PARAM, LIST_TYPE_PARAM_VAL);

		if (startAfter != null) {

			String actualStartAfter;

			if (startAfter.startsWith(normalizedPrefix)) {
				actualStartAfter = startAfter;
			} else {
				actualStartAfter = normalizedPrefix + startAfter;
			}

			queryParameters.put(START_AFTER_PARAM, actualStartAfter);
		}

		if (contToken != null) {
			queryParameters.put(CONT_TOKEN_PARAM, contToken);
		}

		if (normalizedPrefix != null) {
			queryParameters.put(PREFIX_PARAM, normalizedPrefix);
		}

		if ((sessionToken != null) && (!sessionToken.isBlank())) {
			headers.put(AWS_SECURITY_TOKEN, sessionToken);
		}

		String authorization = signer.computeSignature(headers, queryParameters,
			AWS4SignerBase.EMPTY_BODY_SHA256, accessKey, secretKey);

		headers.put(AUTHORIZATION_HEADER, authorization);

		URI endpointUri = addQueryParams(baseEndpointUrl.toURI(), queryParameters);

		HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().
			uri(endpointUri).
			GET();

		for (Map.Entry<String, String> entry : headers.entrySet()) {
			requestBuilder.header(entry.getKey().toLowerCase(), entry.getValue());
		}

		HttpRequest request = requestBuilder.build();

		if (logger.isDebugEnabled()) {
			logger.debug("listObjects request: " + request);
		}

		HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());

		int statusCode = response.statusCode();
		String body = response.body();

		if (logger.isDebugEnabled()) {
			logger.debug("listObjects response status: " + statusCode + ", body: " + body);
		}

		if (shouldFail(statusCode, failOnMissing, body)) {

			String errorMsg = parseS3ErrorMessage(body, statusCode);

			throw new IOException(
					String.format("S3 ListObjectsV2 failed. Bucket: '%s', Prefix: '%s', Status: %d, Error: %s",
							this.indexBucketName, normalizedPrefix, statusCode, errorMsg));
		}

		if ((statusCode == HttpURLConnection.HTTP_NOT_FOUND) ||
			(body == null) ||
			(body.isEmpty())) {

			if (logger.isDebugEnabled()) {
				logger.debug("Empty response body from S3 ListObjectsV2 for prefix: " + normalizedPrefix);
			}

			// S3 should always return XML, but handle defensively
			return new ListObjectsResponseOutput(new ArrayList<>(), null, false);
		}

		try {
			return readResponseKeys(body);
		} catch (Exception e) {
			String truncatedBody = body.length() > 500 ? body.substring(0, 500) + "..." : body;
			throw new IllegalStateException(
				String.format("Failed to parse S3 ListObjectsV2 response. Bucket: '%s', Prefix: '%s', " +
						"ContinuationToken: '%s', Response (truncated): %s",
						this.indexBucketName, normalizedPrefix, contToken, truncatedBody), e);
		}
	}

	private static boolean shouldFail(int statusCode, boolean failOnMissing, String body) {
		if (statusCode == HttpURLConnection.HTTP_OK) {
			return false;
		}
		
		if (statusCode != HttpURLConnection.HTTP_NOT_FOUND) {
			return true;
		}
		
		if (NO_SUCH_KEY.equals(extractResponseCode(body))) {
			return failOnMissing;
		}
		
		return true;
	}
	
	private static String parseS3ErrorMessage(String body, int statusCode) {

		StringBuilder result = new StringBuilder();
		
		result.append("Status Code:").append(statusCode);
		
		if ((body == null) ||
			(body.isEmpty())) {
			
			result.append(", no response body");
		} else {
			String code = extractResponseCode(body);
			String message = extractResponseMessage(body);
			
			result.append(", Code:").append(code).append(", Message:").append(message);
		}
		
		return result.toString();
	}

	private static String extractResponseCode(String xml) {
		return extractXmlTag(xml, RESPONSE_CODE_OPEN, RESPONSE_CODE_CLOSE);
	}

	private static String extractResponseMessage(String xml) {
		return extractXmlTag(xml, RESPONSE_MESSAGE_OPEN, RESPONSE_MESSAGE_CLOSE);
	}

	private static String extractXmlTag(String xml, String open, String close) {

		int startIndex = xml.indexOf(open);

		if (startIndex == -1) {
			return null;
		}

		int endIndex = xml.indexOf(close, startIndex);

		if (endIndex == -1) {
			return null;
		}

		return xml.substring(startIndex + open.length(), endIndex);
	}
	
	private ListObjectsResponseOutput readResponseKeys(String body) {
		
		String value = XmlEscape.unescapeXml(body);
		
		int contTokenOpen = value.indexOf(NEXT_CONT_TOKEN_OPEN);
		int contTokenClose = (contTokenOpen != -1) ?
			value.indexOf(NEXT_CONT_TOKEN_CLOSE, contTokenOpen) : 
			-1;
	
		int isTruncOpen = value.indexOf(IS_TRUNC_OPEN);
		int isTruncClose = (isTruncOpen != -1) ?
			value.indexOf(IS_TRUNC_CLOSE, isTruncOpen) : 
			-1;	
		
		Boolean isTrunc = (isTruncOpen != -1) && (isTruncClose != -1) ?
			Boolean.valueOf(value.substring(isTruncOpen + IS_TRUNC_OPEN.length(), isTruncClose)) :
			null;
		
		if (isTrunc == null) {
			throw new IllegalStateException(IS_TRUNC + " not found");
		}
		
		String contToken = (contTokenOpen != -1) && (contTokenClose != -1) ?
			value.substring(contTokenOpen + NEXT_CONT_TOKEN_OPEN.length(), contTokenClose) :
			null;
		
		List<String> keys = new ArrayList<>();
		
		int keyOpen;
		int keyClose = isTruncClose;
		
		do {
			
			keyOpen = value.indexOf(LIST_KEY_OPEN, keyClose);
			keyClose = (keyOpen != -1) ?
				value.indexOf(LIST_KEY_CLOSE, keyOpen) : 
				-1;
			
			String key = (keyOpen != -1) && (keyClose != -1) ?
				value.substring(keyOpen + LIST_KEY_OPEN.length(), keyClose) :
				null;
			
			if (key != null) {
				CharSequence unescaped = unescapeKey(key);
				CharSequence noPrefix = unescaped.subSequence(indexBucketPath.length(), unescaped.length());
				
				keys.add(noPrefix.toString());
			}
			
		} while ((keyOpen != -1) && (keyClose != -1));
				
		if (logger.isDebugEnabled()) {
			
			logger.debug("listObjects keys: " + keys +
				", contToken: " + contToken + ", isTrunc: " + isTrunc);
		}
		
		return new ListObjectsResponseOutput(keys, contToken, isTrunc.booleanValue());
	}
	
	protected void putObjectAPI(String objectKey, InputStream inputStream,
		long contentLength, Map<String, String> tags) {

		PutObjectRequest.Builder putRequestBuilder = PutObjectRequest.builder().
				bucket(this.indexBucketName).
				key(this.indexBucketPath + objectKey);
		
		if (tags != null) {

			Collection<Tag> tagList = new ArrayList<>(tags.size());
			
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				
				String key = entry.getKey();
				String value = entry.getValue();
				
				if (value.length() > MAX_AWS_TAG_LEN) {
	
					if (logger.isDebugEnabled()) {
						logger.debug("Tag {} of {} is over {} chars, will truncate ({}).",
							key, objectKey, MAX_AWS_TAG_LEN, value);
					}
	
					value = value.substring(0, MAX_AWS_TAG_LEN);
				}
	
				tagList.add(
					Tag.builder().
						key(key).
						value(value).
						build()
				);
			}
			
			putRequestBuilder.tagging(Tagging.builder().tagSet(tagList).build());
		}
		
		PutObjectRequest putRequest = putRequestBuilder.build();
	
		if (logger.isDebugEnabled()) {
			logger.debug("listObjects putRequest: " + putRequest);
		}
		
		AsyncRequestBody body = (contentLength > 0)
				? AsyncRequestBody.forBlockingInputStream(contentLength)
				: AsyncRequestBody.fromString("");

		CompletableFuture<PutObjectResponse> request = this.clients.s3AsynClient()
				.putObject(putRequest, body)
				.whenComplete((response, err) -> {
					if (response != null) {
						logger.debug("put object response: {}", response);
					} else {
						logger.error("put object failure: {}", err);
					}
				});
		
		if (contentLength > 0) {
			((BlockingInputStreamAsyncRequestBody)body).writeInputStream(inputStream);
		}

		this.trackAsyncRequest(request);
	}
	
	private void putObject(String objectKey, InputStream inputStream, 
		long contentLength, Map<String, String> tags) {

		this.putObjectAPI(objectKey, inputStream, contentLength, tags);
	}
	
	@Override
	public String putObject(String prefix, String key, 
		InputStream content, long contentLength,
		Map<String, String> tags) throws IOException {
					
		String fullKey = String.join(this.keyPathSeperator(), prefix, escapeKey(key));
			
		try {
			
			this.putObject(fullKey, content, contentLength, tags);
			
		} catch (Exception e) {
			
			throw new IllegalStateException("error putting object: " + fullKey + 
				"contentLength: " + contentLength  + " tags:" + tags, e);
		}
		
		return fullKey;
	}

	@Override
	public InputStream readObject(String key) throws IOException {
		
		GetObjectRequest request = GetObjectRequest.builder().
			bucket(this.indexBucketName).
			key(this.indexBucketPath + key).
			build();
	     
		try {
			
			return this.clients.s3Client().getObject(request);	
			
		} catch (Exception e) {
			
			throw new IllegalStateException("could not read index object: " + key, e);
		}
	}
	
	@Override
	public Iterator<List<String>> iterateObjectKeys(String prefix, String searchAfter, boolean failOnMissing) {
		return new S3IndexIterator(prefix, searchAfter, failOnMissing);
	}

	@Override
	public InputStream readObject(String key, long off, int len) 
		throws IOException {

		GetObjectRequest.Builder builder = GetObjectRequest.builder().
			bucket(options.inputContainer()).
			key(key);
		
		String range = ((off != -1) && (len != -1)) ?
			String.format("bytes=%d-%d", off, off + len) :
			null;
		
		if (range != null) {
			builder.range(range);
		}
	    
		GetObjectRequest request = builder.build();
		
		if (!USE_LAZY_STREAM) {
			return this.clients.s3Client().getObject(request);
		}
		
		boolean copyToByteArray = ((range != null) && (LAZY_STREAM_COPY_TO_BAIS));
		
		return new LazyInputStream(() -> {
			
			try {
				return this.clients.s3Client().getObject(request);
			} catch (Exception e) {
				throw new IllegalStateException("could not read target object: " + key +
						((range != null) ? (" " + range) : ""), e);
			}
			
		}, copyToByteArray);
	}
	
	@Override
	public int keyByteLength(CharSequence key) {

		String fullKey = String.join(keyPathSeperator(), this.indexBucketPath, key);

		return isValidS3Key(fullKey);
	}
	
	public static int isValidS3Key(CharSequence key) {
		
		CharSequence value = escapeKey(key);

		int len = Utf8.encodedLength(value);
		
		return (len < MAX_S3_KEY_BYTES) ? 
			len : 
			-1;
	}
	
	@Override
	public String keyPathSeperator() {
		return SEPARATOR;
	}

	@Override
	public void invoke(URI url, String body) {

		switch (this.endpointType) {
			case LAMBDA:
				this.lambdaInvoke(url.toString(), body);
				break;

			case SQS:
				this.sqsSendMessage(url.toString(), body);
				break;

			case HTTP:
				ObjectStorageIndexAccessor.super.invoke(url, body);
				break;

			default:
				throw new IllegalStateException("Unsupported endpoint type: " + this.endpointType);
		}
	}
	
	private void lambdaInvoke(String lambdaName, String body) {

		InvokeRequest invokeRequest =
				InvokeRequest.builder()
					.functionName(lambdaName)
					.payload(SdkBytes.fromUtf8String(body))
					.invocationType(InvocationType.EVENT)
					.build();

		CompletableFuture<InvokeResponse> future = this.clients.lambdaClient()
				.invoke(invokeRequest)
				.whenComplete((response, err) -> {
					if (response != null) {
						logger.debug("invoke lambda response: {}", response);
					} else {
						logger.error("invoke lambda failure: {}", err);
					}
				});

		this.trackAsyncRequest(future);
	}

	/**
	 * Maximum length of message body to include in error logs.
	 */
	private static final int MAX_LOG_BODY_LENGTH = 1000;

	private void sqsSendMessage(String queueName, String body) {

		// O13 — propagate W3C trace context across the SQS hop. If the
		// current thread has a traceparent set (log4j2 ThreadContext for the
		// pipeline runtime, or SLF4J MDC for request plumbing), attach it
		// as an SQS MessageAttribute so the consuming worker can resume the
		// span. If absent, omit the attribute — receivers tolerate missing
		// context.
		String traceparent = org.apache.logging.log4j.ThreadContext.get("traceparent");
		if (traceparent == null || traceparent.isEmpty()) traceparent = org.slf4j.MDC.get("traceparent");
		String tracestate = org.apache.logging.log4j.ThreadContext.get("tracestate");
		if (tracestate == null || tracestate.isEmpty()) tracestate = org.slf4j.MDC.get("tracestate");

		SendMessageRequest.Builder builder = SendMessageRequest.builder()
				.queueUrl(queueName)
				.messageBody(body);

		if ((traceparent != null) && (!traceparent.isEmpty())) {
			java.util.Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> attrs = new java.util.HashMap<>();
			attrs.put("traceparent", software.amazon.awssdk.services.sqs.model.MessageAttributeValue.builder()
				.dataType("String").stringValue(traceparent).build());
			if ((tracestate != null) && (!tracestate.isEmpty())) {
				attrs.put("tracestate", software.amazon.awssdk.services.sqs.model.MessageAttributeValue.builder()
					.dataType("String").stringValue(tracestate).build());
			}
			builder.messageAttributes(attrs);
		}

		SendMessageRequest sendMessageRequest = builder.build();

		CompletableFuture<SendMessageResponse> future = this.clients.sqsClient()
				.sendMessage(sendMessageRequest)
				.whenComplete((response, err) -> {
					if (response != null) {
						logger.debug("send SQS message response: {}", response);
					} else {
						int bodySize = (body != null) ? body.length() : 0;
						String truncatedBody = (body != null && body.length() > MAX_LOG_BODY_LENGTH)
							? body.substring(0, MAX_LOG_BODY_LENGTH) + "..."
							: body;
						logger.error("send SQS message failure. Queue: {}, Body size: {} bytes, Body (truncated): {}, Error: {}",
							queueName, bodySize, truncatedBody, err);
					}
				});

		this.trackAsyncRequest(future);
	}

	@Override
	public void close() {
		// First, do a final cleanup to remove any already completed futures
		cleanupCompletedRequests();

		// Wait for all remaining in-flight requests to complete
		// ConcurrentLinkedQueue iterator is weakly consistent and won't throw ConcurrentModificationException
		for (CompletableFuture<?> future : this.inFlightAsyncRequests) {
			try {
				future.join();
			} catch (Exception e) {
				logger.warn("Exception while waiting for async request to complete during close", e);
			}
		}

		// Clear the queue after all operations complete
		this.inFlightAsyncRequests.clear();

		CLIENTS_CACHE.release(this.uuid);
	}

	@Override
	public void logQueryEvent(String queryID, String workerID, QueryLogLevel level, String message, Map<String, Object> metadata) {
		
		if ((this.cloudwatchLogGroup == null) || (this.cloudwatchLogGroup.isBlank())) {
			return;
		}
		
		this.queryEventLog.log(this.cloudwatchLogGroup, queryID, workerID, level, message, metadata);
	}

	@Override
	public void logQueryEvent(String queryID, String workerID, String message) {
		if ((this.cloudwatchLogGroup == null) || (this.cloudwatchLogGroup.isBlank())) {
			return;
		}
		
		this.queryEventLog.log(this.cloudwatchLogGroup, queryID, workerID, message);
	}

	@Override
	public int deleteObjects(List<String> keys) {

		int size = keys.size();
		
		if (size > MAX_S3_DEL_OBJECTS) {
			
			int from = 0;
			int result = 0;
			
			while (from < size) {
			
				from = 0;
				
				int to = (from + MAX_S3_DEL_OBJECTS) > size ?
					size :
					from + MAX_S3_DEL_OBJECTS;
				
				result += this.deleteObjects(keys.subList(from, to));
				
				from = to;
			}
			
			return result;
		}
		
		List<ObjectIdentifier> objectIdentifiers = Arrays.stream(keys.toArray(new String[0])).
			map(key -> ObjectIdentifier.builder().key(this.indexBucketPath + key).build()).
			collect(Collectors.toList());
					
		DeleteObjectsRequest request = DeleteObjectsRequest.builder().
			bucket(this.indexBucketName).
			delete(Delete.builder().objects(objectIdentifiers).build()).
			build();

		DeleteObjectsResponse response = clients.s3Client().deleteObjects(request);
			
		int errorSize = response.errors().size();
		
		int result = response.deleted().size();
		
		if (errorSize > 0) {
						
			logger.warn("deletion errors" +
				". errors.size: " + errorSize +
				", requests.size: " + objectIdentifiers.size() +
				", response.size: " + result +
				", deleted: " + (logger.isDebugEnabled() ? response.deleted() : "DEBUG"),
				", errors: "  + (logger.isDebugEnabled() ? response.errors()  : "DEBUG")
			);
		}
	
		return result;
	}

	public static interface AWSClientsCache {
		public AWSClients get(String uuid);

		public void release(String uuid);
	}

	protected static class AWSClientsRefCountCache implements AWSClientsCache {

		private static class ClientsEntry {
			private final AWSClients clients;
			private final AtomicInteger refCount;

			ClientsEntry() {
				this.clients = new AWSClients();
				this.refCount = new AtomicInteger();
			}
		}

		private final Object lock = new Object();
		private final Map<String, ClientsEntry> map = new HashMap<>();

		@Override
		public AWSClients get(String uuid) {
			synchronized (lock) {

				ClientsEntry result = map.get(uuid);

				if (result != null) {
					result.refCount.incrementAndGet();
					return result.clients;
				}

				result = new ClientsEntry();
				result.refCount.incrementAndGet();

				map.put(uuid, result);

				return result.clients;
			}
		}

		@Override
		public void release(String uuid) {
			AWSClients target = null;

			synchronized (lock) {

				ClientsEntry entry = map.get(uuid);

				if ((entry != null) &&
					(entry.refCount.decrementAndGet() == 0)) {

					map.remove(uuid);
					target = entry.clients;
				}
			}

			if (target != null) {
				target.close();
			}
		}
	}
	
	public static class AWSClients {

		/**
		 * Environment variable to enable S3 path-style access.
		 * Set to "true" when using S3-compatible services (LocalStack, MinIO, etc.)
		 * that require path-style URLs (endpoint/bucket) instead of virtual-hosted
		 * style URLs (bucket.endpoint).
		 */
		private static final String S3_PATH_STYLE_ENV = "TENX_S3_PATH_STYLE";

		private volatile S3Client s3Client;

		private volatile S3AsyncClient s3AsynClient;

		private volatile LambdaAsyncClient lambdaClient;

		private volatile SqsAsyncClient sqsClient;

		private volatile AwsCredentialsProvider credentialsProvider;

		private volatile QueryEventLog queryEventLog;

		/**
		 * Returns S3 path-style access setting from environment variable.
		 */
		private static boolean isS3PathStyleEnabled() {
			return "true".equalsIgnoreCase(System.getenv(S3_PATH_STYLE_ENV));
		}

		public S3Client s3Client() {

			if (this.s3Client == null) {
				synchronized (this) {
					if (this.s3Client == null) {
						this.s3Client = S3Client.builder()
							.forcePathStyle(isS3PathStyleEnabled())
							.build();
					}
				}
			}

			return this.s3Client;
		}

		public S3AsyncClient s3AsynClient() {

			if (this.s3AsynClient == null) {
				synchronized (this) {
					if (this.s3AsynClient == null) {
						if (isS3PathStyleEnabled()) {
							// Path-style requires explicit builder configuration
							this.s3AsynClient = S3AsyncClient.builder()
								.forcePathStyle(true)
								.httpClientBuilder(AwsCrtAsyncHttpClient.builder())
								.build();
						} else {
							// Use optimized CRT client for standard AWS
							this.s3AsynClient = S3AsyncClient.crtCreate();
						}
					}
				}
			}

			return this.s3AsynClient;
		}
		
		public LambdaAsyncClient lambdaClient() {
			if (this.lambdaClient == null) {
				synchronized (this) {
					if (this.lambdaClient == null) {
						this.lambdaClient = LambdaAsyncClient.builder()
								.httpClientBuilder(AwsCrtAsyncHttpClient.builder())
								.build();
					}
				}
			}

			return this.lambdaClient;
		}

		public SqsAsyncClient sqsClient() {
			if (this.sqsClient == null) {
				synchronized (this) {
					if (this.sqsClient == null) {
						this.sqsClient = SqsAsyncClient.builder()
								.httpClientBuilder(AwsCrtAsyncHttpClient.builder())
								.build();
					}
				}
			}

			return this.sqsClient;
		}
		
		public AwsCredentialsProvider credentialsProvider() {
			if (this.credentialsProvider == null) {
				synchronized (this) {
					if (this.credentialsProvider == null) {
						// Use AWS SDK's default credentials provider chain which supports:
						// - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
						// - Web Identity Token (IRSA for K8s via AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN)
						// - Container credentials (ECS)
						// - Instance profile credentials (EC2)
						// - And more...
						this.credentialsProvider = DefaultCredentialsProvider.builder().build();
					}
				}
			}

			return this.credentialsProvider;
		}

		public QueryEventLog queryEventLog() {

			if (this.queryEventLog == null) {
				synchronized (this) {
					if (this.queryEventLog == null) {
						this.queryEventLog = new QueryEventLog();
					}
				}
			}

			return this.queryEventLog;
		}

		/**
		 * Closes all AWS SDK clients and releases resources.
		 * This method is called by the cache when the reference count reaches zero,
		 * or can be called directly by long-lived caches during application shutdown.
		 */
		public void close() {
			if (this.s3Client != null) {
				this.s3Client.close();
			}

			if (this.s3AsynClient != null) {
				this.s3AsynClient.close();
			}

			if (this.lambdaClient != null) {
				this.lambdaClient.close();
			}

			if (this.sqsClient != null) {
				this.sqsClient.close();
			}

			if (this.credentialsProvider != null) {
				// Close the credentials provider to release any resources
				if (this.credentialsProvider instanceof SdkAutoCloseable) {
					((SdkAutoCloseable) this.credentialsProvider).close();
				}
			}

			if (this.queryEventLog != null) {
				this.queryEventLog.shutdown();
			}
		}
	}
}
