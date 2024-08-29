package com.log10x.ext.cloud.index.util.reader.term;

import java.io.InputStream;

/**
 * A Reader for filtering of lines from an input stream based on 
 * a target search term
 */
public class SingleTermFilterReader extends InputTermFilterReader {

	private final char[] term;

	public SingleTermFilterReader(InputStream inputStream, String term) {
		
		super(inputStream);
		this.term = term.toCharArray();
	}
	
	@Override
	protected boolean appendPendingChars(int start, int length) {
		
		if (this.indexOf(this.term, start, length) == -1) {
			return false;
		}

		return super.appendPendingChars(start, length);
	}
}
