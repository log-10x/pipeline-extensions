package com.log10x.ext.cloud.index.util.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.log10x.ext.edge.util.StringBuilderReader;

/**
 * Utility base class for processing lines read from stream. Descendants
 * can use this to decorate and filter lines.
 */
public abstract class BufferedLinesReader extends Reader {
			
	protected static final int LINE_BREAK_CHAR_LEN = 1;
	
	protected static final char LINE_BREAK_CHAR = '\n';
	
	protected static final char CARRIAGE_RET_CHAR = '\r';

	protected StringBuilderReader currLinesChars;

	protected StringBuilder pendingLineChars;

	protected InputStreamReader reader;
		
	protected char[] buffer;
										
	public BufferedLinesReader(InputStream inputStream) {
					
		this.buffer = new char[1024 * 8];	
		this.reader = new InputStreamReader(inputStream);	
		
		this.currLinesChars = new StringBuilderReader();
		this.pendingLineChars = new StringBuilder();				
	}
	
	protected void appendLine(int currLineStart, int currLineLength) {
		
		currLinesChars.builder.
			append(this.buffer, currLineStart, currLineLength).
			append(System.lineSeparator());
	}
		
	/**
	 * Appends the pending line chars to the current line chars, and resets pending. 
	 * 
	 * @param start position in original buffer which is direct continuation of the content
	 * in pending chars.
	 * 
	 * @param len length of the additional line part
	 */
	protected boolean appendPendingChars(int start, int len) {
				
		int pendingLineCharsLen = pendingLineChars.length();
		
		if (pendingLineCharsLen > 0) {
		
			int crLen = pendingLineChars.charAt(pendingLineCharsLen - 1) == CARRIAGE_RET_CHAR ?
				1 : 0;
		
			currLinesChars.builder.append(this.pendingLineChars,
				0, pendingLineCharsLen - crLen);	
			
			pendingLineChars.setLength(0);
		}
					
		return true;
	}
		
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		
		if (this.reader == null) {
			throw new IOException("closed");
		}

		int remaining = currLinesChars.remaining();

		if (remaining > 0) {
			return currLinesChars.read(cbuf, off, len);
		}

		currLinesChars.reset();

		int toRead = Math.min(buffer.length, len);
		int readLen = reader.read(this.buffer, 0, toRead);

		if (readLen == -1) {

			if (pendingLineChars.isEmpty()) {
				return -1;
			}

			if (!this.appendPendingChars(0, 0)) {
				return -1;
			}

			return this.read(cbuf, off, len);
		}

		int prevLineBreakIndex = -1;

		for (int i = 0; i < readLen; i++) {

			char c = this.buffer[i];

			if (c == LINE_BREAK_CHAR) {
					
				int carriageRetLen  = 
					(i > 0) && (this.buffer[i - 1] == CARRIAGE_RET_CHAR)
						? LINE_BREAK_CHAR_LEN
						: 0;

				int currLineStart = prevLineBreakIndex + LINE_BREAK_CHAR_LEN;
				int currLineLength = i - (currLineStart + carriageRetLen);
				
				if (this.appendPendingChars(currLineStart, currLineLength)) {

					this.appendLine(currLineStart, currLineLength);
				} else {

					this.pendingLineChars.setLength(0);
				}

				prevLineBreakIndex = i;
			}
		}

		int currLineStart = prevLineBreakIndex + LINE_BREAK_CHAR_LEN;

		pendingLineChars.append(this.buffer, currLineStart, readLen - currLineStart);

		return this.read(cbuf, off, len);			
	}
	
	@Override
	public void close() throws IOException {

		try {
			
			this.pendingLineChars = null;
			this.currLinesChars = null;
			this.buffer = null;
			
			reader.close();
			
		} finally {
			
			this.reader = null;				
		}	
	}
}
