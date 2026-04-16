package com.log10x.ext.cloud.index.util.reader;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import com.log10x.ext.cloud.index.interfaces.InputStreamLinebreaks;
import com.log10x.ext.cloud.index.util.utf.Utf8Array;

/**
 * A utility filter Reader for marking line breaks within an input char stream.
 * Line breaks are made available via the {@link InputStreamLinebreaks} interface
 */
public class LinebreakReader extends FilterReader implements InputStreamLinebreaks {

	private static final int LINEBREAK_BUF_SIZE = 1024 * 64;

	private final List<long[]> bufList;
	
	private int curIndex;
	
	private long[] curBuff;
		
	private long bytesRead;
	
	private int lastDelIndex;
				
	public LinebreakReader(Reader in) {
		super(in);
		this.bufList = new ArrayList<>();
	}

	private void markPos(long pos) {
		
		synchronized (this.bufList) {
		
			if ((this.curBuff == null) || 
				(this.curIndex == curBuff.length)) {
				
				this.curBuff = new long[LINEBREAK_BUF_SIZE];
				this.curIndex = 0;
				
				bufList.add(curBuff);
			}
			
			this.curBuff[curIndex++] = pos;
		}
	}

	@Override
	public void deleteTo(long index) {
		
		synchronized (this.bufList) {
			
			int delSize  = (int)(index / LINEBREAK_BUF_SIZE);
							
			for (int i = this.lastDelIndex; i < delSize; i++) {
				bufList.set(i, null);		
			}
			
			this.lastDelIndex = delSize;
		}
	}

	@Override
	public long pos(long index) {
		
		synchronized (this.bufList) {
			
			int size = bufList.size();
			
			if (size == 0) {
				return 0;
			}
			
			int bufIndex   = (int)(index / LINEBREAK_BUF_SIZE);
			int indexInBuf = (int)(index % LINEBREAK_BUF_SIZE);
			
			long[] buf = bufList.get(bufIndex);
			
			return buf[indexInBuf];
		}
	}
	
	@Override
	public long endPos() {
		return this.bytesRead;
	}
	
	@Override
	public synchronized int read(char[] cbuf, int off, int len) throws IOException {
		
		int result = super.read(cbuf, off, len);
		
		if (result > 0) {
			
			int lastLinebreakIndex = off - 1;

			for (int i = off; i < off + result; i++) {
				
				char c = cbuf[i];
				
				if (c == '\n') {
										
					int lineBytes = Utf8Array.encodedLength(cbuf, 
						lastLinebreakIndex + 1, i - lastLinebreakIndex);
										
					lastLinebreakIndex = i;
	
					this.bytesRead += lineBytes;
					
					this.markPos(this.bytesRead);
				}				
			}
		
			int from = (lastLinebreakIndex != -1) ?
				lastLinebreakIndex + 1 :
				off;
			
			if (from < off + result) {
				
				this.bytesRead += Utf8Array.encodedLength(cbuf, from, 
					result - (from - off));
			}
			
		}
		
		return result;
	}
}