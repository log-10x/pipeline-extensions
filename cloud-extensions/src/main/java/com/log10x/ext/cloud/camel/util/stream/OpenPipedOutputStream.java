package com.log10x.ext.cloud.camel.util.stream;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class OpenPipedOutputStream extends PipedOutputStream {

	private volatile boolean allowClose;

	public OpenPipedOutputStream(PipedInputStream snk) throws IOException {
		super(snk);
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		super.write(b, off, len);
	}

	@Override
	public void close() throws IOException {
		if (this.allowClose) {
			super.close();
		}
	}

	public void terminate() throws IOException {
		this.allowClose = true;

		close();
	}
}
