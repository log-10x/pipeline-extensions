package com.amazonaws.services.s3.sample.auth;

public class AWSConsts {

	public static final String AWS_CONTENT_SHA_HEADER = "x-amz-content-sha256";
	public static final String AWS_TARGET_HEADER = "X-Amz-Target";
	public static final String AUTHORIZATION_HEADER = "Authorization";
	public static final String AWS_TAGGING_HEADER = "x-amz-tagging";
	public static final String AWS_SECURITY_TOKEN = "x-amz-security-token";
	
	public static final String AWS_SERVICE_PROP = "awsService";
	public static final String AWS_REQUEST_PROP = "awsRequest";
	public static final String AWS_HOST_PROP = "awsHost";
	public static final String AWS_REGION_PROP = "awsRegion";
	public static final String AWS_SECRET_KEY_PROP = "awsSecretKey";
	public static final String AWS_ACCESS_KEY_ID_PROP = "awsAccessKeyID";
	
	public static final String HTTP_METHOD = "httpMethod";
	public static final String HTTP_PUT_METHOD = "PUT";
	public static final String HTTP_GET_METHOD = "GET";
	public static final String CONTENT_LEN_HEADER = "content-length";

	public static final String AWS_REGION_ENV = "AWS_REGION";
	public static final String AWS_SECRET_ACCESS_KEY_ENV = "AWS_SECRET_ACCESS_KEY";
	public static final String AWS_ACCESS_KEY_ID_ENV = "AWS_ACCESS_KEY_ID";
	public static final String AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN";
	
	public static final String S3_SERVICE = "s3";
	public static final String AWS_HOST = "amazonaws.com";
	
	public static final String LIST_KEY = "Key";
	public static final String NEXT_CONT_TOKEN = "NextContinuationToken";
	public static final String IS_TRUNC = "IsTruncated";
	public static final String CONTENTS = "Contents";
	public static final String LAST_MODIFIED = "LastModified";

	public static final String LIST_TYPE_PARAM = "list-type";
	public static final String LIST_TYPE_PARAM_VAL = "2";
	public static final String PREFIX_PARAM = "prefix";
	public static final String START_AFTER_PARAM = "start-after";
	public static final String CONT_TOKEN_PARAM = "continuation-token";
}
