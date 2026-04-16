package com.log10x.ext.edge.output.proc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.edge.output.proc.ProcStreamOptions.ProcDestroyMode;

/**
 * A output stream for wiring tenxObject values into the stdin pipe
 * of a spawned sub-process. This is commonly used to write event
 * data into a forwarder process such as Fluentd, FileBeat, Fluent Bit.. .. 
 * 
 * It allows for caching of the spawned sub-process across multiple 
 * executions of host 10x pipeline with a host JVM.
 * 
 * For an example of how this class is used to connect the host 10x pipeline
 * to a Fluent Bit forwarder, see: {@link https://github.com/log-10x/config/blob/main/pipelines/run/config/output/event/process/config.yaml}
 * 
**/
public class ProcOutputStream extends OutputStream {
			
	protected static final long MAX_STARTUP_WAIT_MS = TimeUnit.SECONDS.toMillis(10);
	
	private final ProcStreamOptions options;
	
	private ProcOutputStreamImpl impl;
	
	/**
	 * this constructor is invoked by the 10x run-time.
	 * 
	 * @param 	args
	 * 			a map arguments of arguments passed to the 10x cli for the
	 * `		target output for which this stream is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this output
	 */
	public ProcOutputStream(Map<String, Object> args, EvaluatorBean evaluatorBean) {
		
		this(MapperUtil.jsonMapper.convertValue(args,
			ProcOutputStreamOptions.class), evaluatorBean);
	}
	
	public ProcOutputStream(ProcStreamOptions options, EvaluatorBean evaluatorBean) {
		
		validateOptions(options);
		
		this.options = options;

		if (options.destroyMode() == ProcDestroyMode.CACHED) {

			this.impl = ProcOutputCache.Instance.get(options, evaluatorBean);
		} else {
			this.impl = ProcOutputStreamImpl.newImpl(options, evaluatorBean);
		}
	}
	
	protected void validateOptions(ProcStreamOptions options) {
		
		if ((options.destroyMode() == ProcDestroyMode.CACHED) &&
			(options.maxCacheSize() <= 0)) {
			
			throw new IllegalArgumentException("Must have positive cache size in cached mode - " + options);
		}

		if (options.startupWaitMs() < 0) {
			
			throw new IllegalArgumentException("'procOutStartupWaitMs' can't be negative - " + options);
		}
		
		if (options.startupWaitMs() > MAX_STARTUP_WAIT_MS) {
			
			throw new IllegalArgumentException("'procOutStartupWaitMs' can't be grater than " + MAX_STARTUP_WAIT_MS + " - " + options);
		}
		
		options.startupWaitPattern();
	}
	
	@Override
	public void write(int b) throws IOException {
	
		if (this.impl == null) {
			throw new IllegalStateException("closed");
		}
			
		if (options.allowMultipleStreams()) {
			
			synchronized (this.impl) {
				impl.write(b);
			}
			
		} else {
			
			impl.write(b);
		}
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		
		if (this.impl == null) {
			throw new IllegalStateException("closed");
		}
		
		if (options.allowMultipleStreams()) {
			
			synchronized (this.impl) {
				impl.write(b, off, len);
			}
			
		} else {
			
			impl.write(b, off, len);
		}
	}
	
	@Override
	public void flush() throws IOException {
		
		if (this.impl == null) {
			throw new IllegalStateException("closed");
		}
		
		if (options.allowMultipleStreams()) {
			
			synchronized (this.impl) {
				impl.flush();
			}
			
		} else {
			
			impl.flush();
		}		
	}
	
	@Override
	public void close() throws IOException {
		
		if (options.allowMultipleStreams()) {
			
			synchronized (this.impl) {
				this.closeImpl();
			}
			
		} else {
			
			this.closeImpl();
		}
	}
	
	private void closeImpl() throws IOException {
		
		if ((this.options.destroyMode() != ProcDestroyMode.CACHED) ||
			(!ProcOutputCache.Instance.put(this.impl))) {

			impl.close();
		}
		
		this.impl = null;
	}
}
