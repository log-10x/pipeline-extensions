package com.log10x.ext.cloud.index.access;

import static com.amazonaws.services.s3.sample.auth.AWSConsts.AUTHORIZATION_HEADER;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_ACCESS_KEY_ID_ENV;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_CONTENT_SHA_HEADER;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_HOST;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_REGION_ENV;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_SECRET_ACCESS_KEY_ENV;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_SECURITY_TOKEN;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_SESSION_TOKEN;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_TAGGING_HEADER;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.AWS_TARGET_HEADER;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.CONTENTS;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.CONT_TOKEN_PARAM;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.HTTP_GET_METHOD;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.HTTP_PUT_METHOD;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.IS_TRUNC;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.LAST_MODIFIED;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.LIST_KEY;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.LIST_TYPE_PARAM;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.LIST_TYPE_PARAM_VAL;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.NEXT_CONT_TOKEN;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.PREFIX_PARAM;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.S3_SERVICE;
import static com.amazonaws.services.s3.sample.auth.AWSConsts.START_AFTER_PARAM;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.unbescape.xml.XmlEscape;

import com.amazonaws.services.s3.sample.auth.AWS4SignerBase;
import com.amazonaws.services.s3.sample.auth.AWS4SignerForAuthorizationHeader;
import com.amazonaws.services.s3.sample.util.BinaryUtils;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageAccessOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.util.stream.LazyInputStream;
import com.log10x.ext.cloud.index.util.utf.Utf8;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.invoke.PipelineLaunchRequest;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

/**
 * Implementation of the {@link ObjectStorageIndexAccessor} interface over AWS services,
 * utilizing S3 buckets and Lambda functions.
 * 
 * Similar implementations can be added for other cloud solutions as Azure and GCP.
 */
public class AWSIndexAccess implements ObjectStorageIndexAccessor {

	/**
	 * When calling {@link #invoke} with a 'url' beginning with this prefix, will assume
	 * the rest of the 'url' is the name of an AWS Lambda which is the invocation target.
	 * 
	 * Otherwise, will assume the 'url' is the invocation target.
	 */
	public static final String ENDPOINT_TYPE_KEY = "invoke";
	
	public static final String ENDPOINT_TYPE_VALUE = "lambda";
	
	private static final String SEPERATOR = String.valueOf('/');
	
	private static final boolean USE_LAZY_STREAM = true;
	private static final boolean LAZY_STREAM_COPY_TO_BAIS = false;
	
	protected final ObjectStorageAccessOptions options;

	private static final Logger logger = LogManager.getLogger(AWSIndexAccess.class);
	
	public static final int MAX_S3_KEY_BYTES = 1024;
	
	private static final String NEXT_CONT_TOKEN_OPEN = xmlNode(NEXT_CONT_TOKEN, false);
	private static final String NEXT_CONT_TOKEN_CLOSE = xmlNode(NEXT_CONT_TOKEN, true);

	private static final String IS_TRUNC_OPEN = xmlNode(IS_TRUNC, false);
	private static final String IS_TRUNC_CLOSE = xmlNode(IS_TRUNC, true);

	private static final String LIST_KEY_OPEN = xmlNode(CONTENTS, false) + xmlNode(LIST_KEY, false) ;
	private static final String LIST_KEY_CLOSE = xmlNode(LIST_KEY, true) + xmlNode(LAST_MODIFIED, false);

	private static final char ESC_TARGET = 0;
	private static final char ESC_TO = 127;
	private static final char ESC_CHAR = '\\';
	
	private static final int MAX_AWS_TAG_LEN = 200;
	
	protected class S3IndexIterator implements Iterator<List<String>> {
				
		private final String prefix;
		
		private final String searchAfter;
		
		private ListObjectsResponseOutput objects;
				
		private int requests;

		protected S3IndexIterator(String prefix, String searchAfter) {
			this.searchAfter = searchAfter;
			this.prefix = prefix;
		}
			
		private void listObjects() {
			
			try {
				
				this.objects = iterateIndexObjects(
						this.prefix,
						this.searchAfter,
						(this.objects != null ? objects.contToken : null));
		
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
	
	private final String accessKey;
	
	private final String secretKey;

	private final String regionName;
	
	private final String sessionToken;
	
	private final String uuid;
	
	private final AWSClients clients;
		
	private final List<CompletableFuture<?>> inFlightAsyncRequests;
	
	private final boolean isLambda;
	
	// this constructor is invoked via reflection
	public AWSIndexAccess(ObjectStorageAccessOptions options, EvaluatorBean evaluatorBean) {
		
		this.options = options;		
		this.accessKey = Objects.requireNonNull(evaluatorBean.env(AWS_ACCESS_KEY_ID_ENV), AWS_ACCESS_KEY_ID_ENV).toString();
		this.secretKey = Objects.requireNonNull(evaluatorBean.env(AWS_SECRET_ACCESS_KEY_ENV), AWS_SECRET_ACCESS_KEY_ENV).toString();
		this.regionName = Objects.requireNonNull(evaluatorBean.env(AWS_REGION_ENV), AWS_REGION_ENV).toString();
		this.sessionToken = System.getenv(AWS_SESSION_TOKEN);
		
		this.uuid = (String)evaluatorBean.env(PipelineLaunchRequest.UNIQUE_ID_ARG);
		
		this.clients = AWSClients.get(this.uuid);
		
		this.inFlightAsyncRequests = Collections.synchronizedList(new LinkedList<>());
		
		this.isLambda = ENDPOINT_TYPE_VALUE.equals(options.args().get(ENDPOINT_TYPE_KEY));
	}

	private static String xmlNode(String name, boolean close) {
		return "<" + (close ? "/" : "") + name + ">";
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
	
	private String bucket() {
		return options.indexContainer();
	}
	
	private String regionURLName() {
		
		return regionName.equals("us-east-1") ?
			"" :
			"-" + this.regionName;		
	}
	
	private URL listObjectsURL() {
				
		StringBuilder uriBuilder = new StringBuilder().
			append("https://").
			append(S3_SERVICE).
			append(this.regionURLName()).
			append(".").
			append(AWS_HOST).
			append("/").
			append(this.bucket());
			
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
		String prefix, String startAfter, String contToken) throws IOException, URISyntaxException, InterruptedException {

		URL baseEndpointUrl = this.listObjectsURL();

		Map<String, String> headers = new HashMap<String, String>(4);		
				
		headers.put(AWS_CONTENT_SHA_HEADER, AWS4SignerBase.EMPTY_BODY_SHA256);
		headers.put(AWS_TARGET_HEADER, this.bucket());

		AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
			baseEndpointUrl, HTTP_GET_METHOD, S3_SERVICE, regionName);

		Map<String, String> queryParameters = new LinkedHashMap<>(4);
		
		queryParameters.put(LIST_TYPE_PARAM, LIST_TYPE_PARAM_VAL);
		
		if (startAfter != null) {
			
			String actualStartAfter;
			
			if (startAfter.startsWith(prefix)) {
				actualStartAfter = startAfter;
			} else {
				actualStartAfter = prefix + this.keyPathSeperator() + startAfter;
			}
			
			queryParameters.put(START_AFTER_PARAM, actualStartAfter);
		}
		
		if (contToken != null) {
			queryParameters.put(CONT_TOKEN_PARAM, contToken);
		}
		
		if (prefix != null) {
			queryParameters.put(PREFIX_PARAM, prefix);
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

		String body = response.body();
		
		if (logger.isDebugEnabled()) {
			logger.debug("listObjects response: " + body);
		}
		
		try {
			return readResponseKeys(body);
		} catch (Exception e) {
			throw new IllegalStateException("could not parse response body: " + body, e);
		}
	}
	
	private static ListObjectsResponseOutput readResponseKeys(String body) {
		
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
				keys.add(unescapeKey(key).toString());
			}
			
		} while ((keyOpen != -1) && (keyClose != -1));
				
		if (logger.isDebugEnabled()) {
			
			logger.debug("listObjects keys: " + keys +
				", contToken: " + contToken + ", isTrunc: " + isTrunc);
		}
		
		return new ListObjectsResponseOutput(keys, contToken, isTrunc.booleanValue());
	}
	
	protected void putObjectAPI(String objectKey, String objectContent, 
		Map<String, String> tags) {

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
		
		PutObjectRequest putRequest = PutObjectRequest.builder().
			bucket(this.bucket()).
			key(objectKey).
			tagging(Tagging.builder().tagSet(tagList).build()).
			build();
	
		if (logger.isDebugEnabled()) {
			logger.debug("listObjects putRequest: " + putRequest);
		}
		
		String body = (objectContent != null) ?
				objectContent : 
				"";
		
		CompletableFuture<PutObjectResponse> request = this.clients.s3AsynClient()
				.putObject(putRequest, AsyncRequestBody.fromString(body))
				.whenComplete((response, err) -> {
					if (response != null) {
						logger.debug("put object response: {}", response);
					} else {
						logger.error("put object failure: {}", err);
					}
				});
		
		this.inFlightAsyncRequests.add(request);
	}
	
	private void putObject(String objectKey, String objectContent, 
		Map<String, String> tags) {

		this.putObjectAPI(objectKey, objectContent, tags);
	}
	
	private static String formatTagHeader(Map<String, String> tags) {
	
		if ((tags == null) || (tags.isEmpty())) {
			return null;
		}
			
		StringBuilder tagBuilder = new StringBuilder();
		
		for (Map.Entry<String, String> entry : tags.entrySet()) {
			
			if (!tagBuilder.isEmpty()) {
				tagBuilder.append("&");
			}
			
			tagBuilder.
				append(entry.getKey()).
				append("=").
				append(entry.getValue());
			
		}
		
		return tagBuilder.toString();
	}

	private URI formatPutObjectURL(String objectKey) throws UnsupportedEncodingException {
		
		String uri = new StringBuilder().	
			append("https://").
			append(this.bucket()).
			append(".").
			append(S3_SERVICE).
			append(this.regionURLName()).
			append(".").
			append(AWS_HOST).
			append('/').
			append(URLEncoder.encode(objectKey, StandardCharsets.UTF_8.toString())).
			toString(); 

		try {
        	
        	return new URI(uri);
        
        } catch (Exception e) {
            throw new RuntimeException("unable to parse service endpoint: " + uri, e);
        }		
	}
	
	protected void putObjectRest(String objectKey, String objectContent, 
		Map<String, String> tags) throws IOException, InterruptedException {
		
		URI endpointUrl = this.formatPutObjectURL(objectKey);
        
        byte[] contentHash = AWS4SignerBase.hash(objectContent);
        String contentHashString = BinaryUtils.toHex(contentHash);

		Map<String, String> headers = new HashMap<String, String>(4);
		
		headers.put(AWS_CONTENT_SHA_HEADER, contentHashString);
		
		String tagHeader = formatTagHeader(tags);
		
		if (tagHeader != null) {
			headers.put(AWS_TAGGING_HEADER, tagHeader);
		}
		
		AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
			endpointUrl.toURL(), HTTP_PUT_METHOD, S3_SERVICE, this.regionName);

		String authorization = signer.computeSignature(headers, null, 
			contentHashString, this.accessKey, this.secretKey);

		headers.put(AUTHORIZATION_HEADER, authorization);

		if (logger.isDebugEnabled()) {
			logger.debug("PutObject request: " + endpointUrl + "headers: " + headers);
		}
		
		HttpClient client = HttpClient.newHttpClient();
		
		HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().
			uri(endpointUrl).
			PUT(BodyPublishers.ofString(objectContent));
				
		for (Map.Entry<String, String> entry : headers.entrySet()) {
			requestBuilder.header(entry.getKey().toLowerCase(), entry.getValue());
		}
		
		HttpRequest request = requestBuilder.build();
		
		HttpResponse<String> response = client.send(request, 
			BodyHandlers.ofString());
		
		int statusCode = response.statusCode();
		
		if (statusCode != HttpURLConnection.HTTP_OK) {
			
			throw new IllegalStateException("error putting object, statusCode: " + 
				statusCode + " body: " + response.body());
			
		} else if (logger.isDebugEnabled()) {
				
			logger.debug("putObject statusCode: 200 body: " + response.body());
		}
	}
	
	@Override
	public String putObject(String prefix, String key, String value,
		Map<String, String> tags) throws IOException {	
					
		String fullKey = String.join(this.keyPathSeperator(), prefix, escapeKey(key));
			
		try {
			
			this.putObject(fullKey, (value != null) ? value : "", tags);
			
		} catch (Exception e) {
			
			throw new IllegalStateException("error putting object: " + fullKey + 
				"value: " + value  + " tags:" + tags, e);
		}
		
		return fullKey;
	}

	@Override
	public InputStream readObject(String key) throws IOException {
		
		GetObjectRequest request = GetObjectRequest.builder().
			bucket(this.bucket()).
			key(key).
			build();
	     
		try {
			
			return this.clients.s3Client().getObject(request);	
			
		} catch (Exception e) {
			
			throw new IllegalStateException("could not read index object: " + key);
		}
	}
	
	@Override
	public Iterator<List<String>> iterateObjectKeys(String prefix, String searchAfter) 
		throws IOException {	
		
		return new S3IndexIterator(prefix, searchAfter);		
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
		return isValidS3Key(key);
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
		return SEPERATOR;
	}

	@Override
	public void invoke(URI url, String body) {
				
		if (this.isLambda) {
						
			this.lambdaInvoke(url.toString(), body);
			
		} else {
			
			ObjectStorageIndexAccessor.super.invoke(url, body);
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
		
		this.inFlightAsyncRequests.add(future);
	}
	
	@Override
	public void close() {
		for (CompletableFuture<?> response : this.inFlightAsyncRequests) {
			response.join();
		}
		
		AWSClients.release(this.uuid);
	}
	
	protected static class AWSClients {
		
		private static final Map<String, AWSClients> map = new HashMap<>();
		
		public static AWSClients get(String uuid) {
			synchronized (AWSClients.class) {
				
				AWSClients result = map.get(uuid);
				
				if (result != null) {
					result.refCount.incrementAndGet();
					return result;
				}
				
				result = new AWSClients();
				result.refCount.incrementAndGet();
				
				map.put(uuid, result);
				
				return result;
			}
		}
		
		public static void release(String uuid) {
			AWSClients target = null;
			
			synchronized (AWSClients.class) {
				
				AWSClients clients = map.get(uuid);
				
				if ((clients != null) &&
					(clients.refCount.decrementAndGet() == 0)) {
					
					map.remove(uuid);
					target = clients;
				}
			}
			
			if (target != null) {
				target.close();
			}
		}
		
		private final AtomicInteger refCount;
		
		private S3Client s3Client;
		
		private S3AsyncClient s3AsynClient;
		
		private LambdaAsyncClient lambdaClient;
		
		private AWSClients() {
			this.refCount = new AtomicInteger();
		}
		
		public S3Client s3Client() {

			if (this.s3Client == null) {
				synchronized (this) {
					if (this.s3Client == null) {
						this.s3Client = S3Client.builder().build();
					}
				}
			}
			
			return this.s3Client;
		}
		
		public S3AsyncClient s3AsynClient() {

			if (this.s3AsynClient == null) {
				synchronized (this) {
					if (this.s3AsynClient == null) {
						this.s3AsynClient = S3AsyncClient.builder()
								.httpClientBuilder(AwsCrtAsyncHttpClient.builder())
								.build();
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
		
		private void close() {
			if (this.s3Client != null) {
				this.s3Client.close();
			}
			
			if (this.s3AsynClient != null) {
				this.s3AsynClient.close();
			}
			
			if (this.lambdaClient != null) {
				this.lambdaClient.close();
			}
		}
	}
}
