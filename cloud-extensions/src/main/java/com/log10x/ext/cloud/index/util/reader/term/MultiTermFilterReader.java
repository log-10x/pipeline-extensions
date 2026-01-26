package com.log10x.ext.cloud.index.util.reader.term;

import java.io.InputStream;
import java.util.List;

/**
 * A Reader for filtering of lines from an input stream based on 
 * a list of search terms
 */
public class MultiTermFilterReader extends InputTermFilterReader {

	private final char[][] terms;

	public MultiTermFilterReader(InputStream inputStream, List<String> terms) {
		
		super(inputStream);
		
		int size = terms.size();

		this.terms = new char[size][];
		
		for (int i = 0; i < size; i++) {
			this.terms[i] = terms.get(i).toCharArray();
		}
	}
	
	@Override
	protected boolean appendPendingChars(int start, int length) {
		
		for (char[] term : this.terms) {
		
			if (this.indexOf(term, start, length) == -1) {
				return false;
			}
		}
				
		return super.appendPendingChars(start, length);
	}
}