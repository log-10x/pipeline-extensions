package com.log10x.ext.cloud.index.util.reader;

import java.io.IOException;
import java.io.Reader;

/**
 * A utility Reader from a {@link StringBuilder} input
 */
public class StringBuilderReader extends Reader {

	public final StringBuilder builder;
	
	private int index;
	
	public StringBuilderReader() {
		this(new StringBuilder());
	}
	
	public StringBuilderReader(String s) {
		this(new StringBuilder(s));
	}
	
	public StringBuilderReader(StringBuilder builder) {
		this.builder = builder;
	}
	
	public int remaining() {
		return builder.length() - this.index;
	}
	
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		
		int remaining = this.remaining();
		
		int result = Math.min(remaining, len);
		
		if (result <= 0) {
			return -1;
		}
		
		builder.getChars(this.index, this.index + result, cbuf, off);
		
		this.index += result;
		
		return result;
	}

	@Override
	public void close() throws IOException {	
	}
	
	@Override
	public void reset() throws IOException {
		this.index = 0;
		builder.setLength(0);
	}
	
	@Override
	public String toString() {
		return "'" + builder + "' (index: " + index + ")";
	}
}
