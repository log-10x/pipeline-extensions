package com.log10x.ext.cloud.index.shared;

import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.unbescape.java.JavaEscape;

import com.log10x.api.eval.EvaluatorBean;

public class TokenSplitter {

	private final BitSet tokenDelimiters;

	private TokenSplitter(BitSet tokenDelimiters) {

		this.tokenDelimiters = tokenDelimiters;
	}

	public List<String> split(String value) {
		List<String> result = new LinkedList<String>();

		fill(value, result);

		return result;
	}

	public void fill(String value, Collection<String> target) {

		int start = 0;
		int length = value.length();

		for (int i = 0; i < length; i++) {
			char c = value.charAt(i);

			if (tokenDelimiters.get(c)) {

				if (i > start) {
					// Only add non-empty strings
					target.add(value.substring(start, i));
				}

				start = i + 1;
			}
		}

		// Add the last segment if non-empty
		if (start < length) {
			target.add(value.substring(start));
		}
	}

	public static TokenSplitter create(EvaluatorBean evaluatorBean) {

		BitSet tokenDelimiters = new BitSet();

		Object raw = evaluatorBean.env("tokenDelims");

		if (raw instanceof String tokenDelims) {

			String escaped = JavaEscape.unescapeJava(tokenDelims);

			for (int i = 0; i < escaped.length(); i++) {
				tokenDelimiters.set(escaped.charAt(i));
			}
		}

		return new TokenSplitter(tokenDelimiters);
	}
}
