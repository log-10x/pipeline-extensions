package com.log10x.ext.cloud.index.write;

import java.util.ArrayList;
import java.util.List;

/**
 * POJO mapping an input object target (e.g. log file) and byte ranges.
 */
public class IndexWriteTarget {

	public final String target;
	public final long targetHash;

	public final List<ByteRange> byteRanges;

	// For reflection
	public IndexWriteTarget() {
		this(null, 0);
	}

	public IndexWriteTarget(String target, long targetHash) {
		this.target = target;
		this.targetHash = targetHash;
		this.byteRanges = new ArrayList<>();
	}

	public static class ByteRange {

		public final long offset;
		public final int length;
		public final long minTimestamp;
		public final long maxTimestamp;

		// For reflection
		public ByteRange() {
			this(0, 0, 0, 0);
		}

		public ByteRange(long offset, int length, long minTimestamp, long maxTimestamp) {
			this.offset = offset;
			this.length = length;
			this.minTimestamp = minTimestamp;
			this.maxTimestamp = maxTimestamp;
		}
		
		@Override
		public int hashCode() {
			return (int)(offset ^ minTimestamp);
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ByteRange)) {
				return false;
			}

			if (this == obj) {
				return true;
			}

			ByteRange other = (ByteRange) obj;

			return ((this.offset == other.offset) &&
					(this.length == other.length) &&
					(this.minTimestamp == other.minTimestamp) &&
					(this.maxTimestamp == other.maxTimestamp));
		}
		
		@Override
		public String toString() {
			return String.format("ByteRange(%d, %d, %d, %d)", offset, length, minTimestamp, maxTimestamp);
		}
	}
}
