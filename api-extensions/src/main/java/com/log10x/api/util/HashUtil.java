package com.log10x.api.util;

public class HashUtil {

	// https://stackoverflow.com/questions/1660501
	//
	public static long hash(CharSequence string) {

		// prime
		long startValue = 1125899906842597L;

		return hash(startValue, string);
	}

	public static long hash(long startValue, CharSequence string) {

		long h = startValue;
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + string.charAt(i);
		}
		return h;
	}
}
