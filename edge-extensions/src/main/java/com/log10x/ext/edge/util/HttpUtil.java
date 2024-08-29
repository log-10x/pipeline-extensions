package com.log10x.ext.edge.util;

import java.util.Base64;

public class HttpUtil {
	public static String basicAuth(String user, String password) {
		String auth = user + ":" + password;

		return "Basic " + Base64.getEncoder().encodeToString(auth.getBytes());
	}

	public static String bearerAuth(String token) {
		return "Bearer " + token;
	}
}
