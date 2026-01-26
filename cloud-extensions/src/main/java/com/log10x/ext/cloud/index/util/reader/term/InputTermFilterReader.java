package com.log10x.ext.cloud.index.util.reader.term;

import java.io.InputStream;

import com.log10x.ext.cloud.index.util.reader.BufferedLinesReader;

/**
 * A base Reader for filtering lines from an input stream
 */
public abstract class InputTermFilterReader extends BufferedLinesReader {

	protected InputTermFilterReader(InputStream inputStream) {
		super(inputStream);
	}

	protected int indexOf(char[] term, int bufferStart, int bufferLen) {

		int pendingLen = pendingLineChars.length();

		int totalAvailable = pendingLen + bufferLen;

		if (term.length > totalAvailable) {
			return -1;
		}

		int lastPossibleStartPosition = totalAvailable - term.length;

		for (int i = 0; i <= lastPossibleStartPosition; i++) {

			boolean match = true;

			for (int j = 0; j < term.length; j++) {

				int searchPos = i + j;

				char c = (searchPos < pendingLen)
						? pendingLineChars.charAt(searchPos)
						: this.buffer[bufferStart + searchPos - pendingLen];

				char t = term[j];

				if (c != t) {
					match = false;
					break;
				}
			}

			if (match) {
				return i;
			}
		}

		return -1;
	}
}
