package com.log10x.ext.edge.output.unix;

import java.io.IOException;
import java.io.OutputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Map;

/**
 * An example output stream used to write tenxObject values to
 * a UNIX domain socket.
 * 
 * For an example of use within an 10x pipeline, see:
 * {@link https://doc.log10x.com/run/output/event/outputStream/}
 */
public class UnixSocketOutputStream extends OutputStream {

	private static final String ADDRESS = "address";
	
	public final String address;
	
	private final SocketChannel channel;
	
	private ByteBuffer buffer;
		
	/**
	 * this constructor is invoked by the 10x run-time.
	 * 
	 * @param 	args
	 * 			a map arguments of arguments passed to the 10x cli for the
	 * `		target output for which this stream is instantiated
	 * 
	 */
	public UnixSocketOutputStream(Map<String, Object> args) throws IOException {
		
		if ((args == null) ||
			(!args.containsKey(ADDRESS))) {
			
			throw new IllegalArgumentException("expected socket address, received: " +
				args);	
		}
		
		this.address = String.valueOf(args.get(ADDRESS));
		
		this.channel = channel(address);	
	}
	
	private void reset(int len) {
		
		if ((this.buffer == null) || 
			(buffer.capacity() < len)) {
			
			this.buffer = ByteBuffer.allocate(Math.min(1024, len));
			
		}
		
		buffer.clear();		
	}
	
	private void write() throws IOException {
		
		buffer.flip();
		
		while (buffer.hasRemaining()) {
		    channel.write(buffer);
		}
	}
	
	@Override
	public synchronized void write(byte[] b, int off, int len) throws IOException {
		
		this.reset(len);
		buffer.put(b, off, len);
		this.write();
	}
	
	@Override
	public synchronized void write(int b) throws IOException {
		
		this.reset(1);
		buffer.put((byte)b);
		this.write();
	}
		
	@Override
	public String toString() {
		return this.address;
	}
	
	@Override
	public void close() throws IOException {
		channel.close();
	}
	
	public static SocketChannel channel(String address) throws IOException {
		
		UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(Path.of(address));
		
		SocketChannel result = SocketChannel.open(StandardProtocolFamily.UNIX);
		
		result.connect(socketAddress);
		
		return result;
	}
}
