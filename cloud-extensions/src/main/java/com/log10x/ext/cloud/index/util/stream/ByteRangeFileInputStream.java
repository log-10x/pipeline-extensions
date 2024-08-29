package com.log10x.ext.cloud.index.util.stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;

/**
 * Utility stream for reading a byte range from a target file.
 */
public class ByteRangeFileInputStream extends FilterInputStream {
	
	private final int length;
	
	private int bytesRead;

	public ByteRangeFileInputStream(File file, long offset, int length) throws IOException {
		super(new FileInputStream(file));
		
		this.length = length;

		FileInputStream f = (FileInputStream)this.in;
		
		f.getChannel().position(offset);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		
		if (this.bytesRead >= this.length) {
			return -1;
		}
		
		int toRead = Math.min(len, this.length - this.bytesRead);
		
		int result = super.read(b, off, toRead);
		
		this.bytesRead += result;
				
		return result;
	}
}
