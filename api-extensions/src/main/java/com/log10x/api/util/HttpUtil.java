package com.log10x.api.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class HttpUtil {
	
	private static volatile String hostname;
	
	public static String basicAuth(String user, String password) {
		
		String auth = user + ":" + password;

		return "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
	}

	public static String bearerAuth(String token) {
		return "Bearer " + token;
	}
	
	private static String getHostname() {
		InetAddress localhost;

		try {
			localhost = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			localhost = null;
		}

		String result = (localhost == null ? "" : localhost.getHostName());

		return (result.isBlank() ? "Unknown" : result);
	}
	
	public static String hostname() {
		
		if (hostname == null) {
			
			synchronized (HttpUtil.class) {
				
				if (hostname == null) {
					hostname = getHostname();
				}
			}
		}
		
		return hostname;
	}
}
