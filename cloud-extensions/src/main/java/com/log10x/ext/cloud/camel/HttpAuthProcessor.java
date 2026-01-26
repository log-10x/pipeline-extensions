package com.log10x.ext.cloud.camel;

import java.util.Base64;

import org.apache.camel.Exchange;
import org.apache.camel.Message;

/**
 * A route process for basic HTTP auth. To learn more, see:
 * {@link https://en.wikipedia.org/wiki/Basic_access_authentication}
 */
public class HttpAuthProcessor extends BaseRouteProcessor {

	public static final String USERNAME = "username";
	
	public static final String PASSWORD = "password";
	
	public static final String AUTHORIZATION = "Authorization";

	@Override
	public void process(Exchange exchange) throws Exception {
		
		String username = routeStrProp(exchange, USERNAME);

		String password = routeStrProp(exchange, PASSWORD);
		
		String combined  = username + ":" + password;
		
		String auth = "Basic " + Base64.getEncoder().encodeToString(combined.getBytes());
		
		Message message = exchange.getMessage();

		message.setHeader(AUTHORIZATION, auth);			
	}
}
