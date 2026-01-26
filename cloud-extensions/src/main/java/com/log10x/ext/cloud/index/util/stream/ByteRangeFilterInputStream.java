package com.log10x.ext.cloud.index.util.stream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Utility stream for reading a byte range list from a target file separated by
 * a new line.
 */
public class ByteRangeFilterInputStream extends FilterInputStream {

	private static final byte[] NEWLINE_BYTES = System.lineSeparator().getBytes();

	private int currPosInByteRange;

	private Iterator<Map.Entry<Long, Integer>> iter;

	private Map.Entry<Long, Integer> currByteRange;

	public ByteRangeFilterInputStream(InputStream in, TreeMap<Long, Integer> byteRanges) {

		super(in);
		this.iter = byteRanges.entrySet().iterator();
	}

	protected int readByteRangeSeperator(byte[] b, int off, int len) {

		int result = Math.min(NEWLINE_BYTES.length, len);
		System.arraycopy(NEWLINE_BYTES, 0, b, off, result);

		return result;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {

		if (this.currByteRange == null) {

			if (!iter.hasNext()) {
				return -1;
			}

			this.currByteRange = iter.next();
			this.currPosInByteRange = 0;
		}

		int currByteRangeLen = currByteRange.getValue();
		int remainingLen = currByteRangeLen - this.currPosInByteRange;
		
		if (remainingLen > 0) {

			int toRead = Math.min(remainingLen, len);
			int result = in.read(b, off, toRead);

			this.currPosInByteRange += result;

			return result;
		}

		if (!iter.hasNext()) {
			return -1;
		}

		Map.Entry<Long, Integer> prevByteRange = this.currByteRange;

		this.currByteRange = iter.next();
		this.currPosInByteRange = 0;

		if (prevByteRange != null) {

			long prevRangeEnd = prevByteRange.getKey() + prevByteRange.getValue();
			long bytesToNextRange = currByteRange.getKey() - prevRangeEnd;

			in.skipNBytes(bytesToNextRange);
		}

		return this.readByteRangeSeperator(b, off, len);
	}
}
