package com.log10x.ext.cloud.index.util.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;

public class LazyInputStream extends FilterInputStream {

	private final Supplier<InputStream> supplier;
	private final boolean copyToByteArray;

	public LazyInputStream(Supplier<InputStream> supplier, boolean copyToByteArray) {
		super(null);

		this.supplier = supplier;
		this.copyToByteArray = copyToByteArray;
	}

	private void validateStream() throws IOException {

		if (this.in != null) {
			return;
		}

		synchronized (this) {

			if (this.in == null) {

				if (!this.copyToByteArray) {

					this.in = supplier.get();
					return;
				}
				
				InputStream original = null;

				try {
					original = supplier.get();

					if (original == null) {
						throw new IOException("No input stream - " + supplier);
					}

					ByteArrayOutputStream temp = new ByteArrayOutputStream();
					IOUtils.copy(original, temp);

					this.in = new ByteArrayInputStream(temp.toByteArray());

				} finally {

					if (original != null) {
						original.close();
					}
				}
			}
		}
	}

	@Override
	public int read() throws IOException {
		validateStream();

		return super.read();
	}

	@Override
	public int read(byte b[], int off, int len) throws IOException {
		validateStream();

		return super.read(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		validateStream();

		return super.skip(n);
	}

	@Override
	public int available() throws IOException {
		validateStream();

		return super.available();
	}

	@Override
	public void close() throws IOException {
		validateStream();

		super.close();
	}

	@Override
	public synchronized void mark(int readlimit) {

		try {
			validateStream();

		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		}

		super.mark(readlimit);
	}

	@Override
	public synchronized void reset() throws IOException {
		validateStream();

		super.reset();
	}

	@Override
	public boolean markSupported() {
		try {
			validateStream();

		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		}

		return super.markSupported();
	}
}
