package com.log10x.ext.edge.output.proc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.json.MapperUtil;
import com.log10x.ext.edge.output.proc.ProcOutputStreamOptions.ProcDestroyMode;

/**
 * A output stream for wiring l1x Object values into the stdin pipe
 * of a spawned sub-process. This is commonly used to write event
 * data into a forwarder process such as Fluentd, FileBeat, Fluent Bit.. .. 
 * 
 * It allows for caching of the spawned sub-process across multiple 
 * executions of host l1x pipeline with a host JVM.
 * 
 * For an example of how this class is used to connect the host l1x pipeline
 * to a Fluent Bit forwarder, see: {@link https://github.com/l1x-co/config/blob/main/pipelines/run/config/output/event/process/fluentbit.yaml}
 * 
**/
public class ProcOutputStream extends OutputStream {
			
	private final ProcOutputStreamOptions options;
	
	private ProcOutputStreamImpl impl;
	
	/**
	 * this constructor is invoked by the l1x run-time.
	 * 
	 * @param 	args
	 * 			a map arguments of arguments passed to the l1x cli for the
	 * `		target output for which this stream is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an l1x evaluator bean which allows this output
	 */
	public ProcOutputStream(Map<String, Object> args, EvaluatorBean evaluatorBean) {
		
		this(MapperUtil.jsonMapper.convertValue(args,
			ProcOutputStreamOptions.class), evaluatorBean);
	}
	
	public ProcOutputStream(ProcOutputStreamOptions options, EvaluatorBean evaluatorBean) {
		
		if ((options.procOutDestroyMode == ProcDestroyMode.CACHED) &&
			(options.procOutMaxCacheSize <= 0)) {
			
			throw new IllegalArgumentException("Must have positive cache size in cached mode - " + options);
		}

		this.options = options;

		if (options.procOutDestroyMode == ProcDestroyMode.CACHED) {

			this.impl = ProcOutputCache.Instance.get(options, evaluatorBean);
		} else {
			this.impl = newImpl(options, evaluatorBean);
		}
	}
	
	@Override
	public void write(int b) throws IOException {
	
		if (this.impl == null) {
			throw new IllegalStateException("closed");
		}
			
		if (options.procOutAllowMultipleStreams) {
			
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
		
		if (options.procOutAllowMultipleStreams) {
			
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
		
		if (options.procOutAllowMultipleStreams) {
			
			synchronized (this.impl) {
				impl.flush();
			}
			
		} else {
			
			impl.flush();
		}		
	}
	
	private void closeImpl() throws IOException {
		
		if ((this.options.procOutDestroyMode != ProcDestroyMode.CACHED) ||
			(!ProcOutputCache.Instance.put(this.impl))) {

			impl.close();
		}
		
		this.impl = null;
	}
	
	@Override
	public void close() throws IOException {
		
		if (options.procOutAllowMultipleStreams) {
			
			synchronized (this.impl) {
				this.closeImpl();
			}
			
		} else {
			
			this.closeImpl();
		}
	}
	
	private static ProcOutputStreamImpl newImpl(ProcOutputStreamOptions options, EvaluatorBean evaluatorBean) {
		
		long destroyWait = (options.procOutDestroyWait != null) ?
				((Number)evaluatorBean.eval(String.format("parseDuration(\"%s\")", 
					options.procOutDestroyWait))).longValue() : 
				0L;
		
		return new ProcOutputStreamImpl(options, destroyWait);
	}
	
	private static class ProcOutputCache {
		private static final ProcOutputCache Instance = new ProcOutputCache();
		
		private final Map<String, LinkedList<ProcOutputStreamImpl>> cache = new HashMap<>(4);
		
		private synchronized ProcOutputStreamImpl get(
				ProcOutputStreamOptions options, EvaluatorBean evaluatorBean) {
			
			LinkedList<ProcOutputStreamImpl> queue = cache.get(options.command());
			
			if (queue != null) {
				
				Iterator<ProcOutputStreamImpl> iter = queue.iterator();
				
				while (iter.hasNext()) {
					
					ProcOutputStreamImpl curr = iter.next();
					
					if (!curr.isAlive()) {
						iter.remove();
						continue;
					}
					
					if (!Objects.equals(
						curr.options.procOutCommand, 
						options.procOutCommand)) {
						
						continue;
					}
					
					if (!Objects.equals(
						curr.options.procOutArgs, 
						options.procOutArgs)) {
							
						continue;
					}
					
					if (!options.procOutAllowMultipleStreams) {
						iter.remove();
					}
					
					return curr;	
				}
			}
			
			ProcOutputStreamImpl result = newImpl(options, evaluatorBean);
			
			put(result);
			
			return result;
		}
	
		private synchronized boolean put(ProcOutputStreamImpl out){
			
			String key = out.options.command();
			
			LinkedList<ProcOutputStreamImpl> queue = cache.get(key);
			
			if (queue == null) {
				queue = new LinkedList<>();
				cache.put(key, queue);
			}
			
			if (queue.contains(out)) {
				return true;
			}
			
			if (queue.size() < out.options.procOutMaxCacheSize) {	
				
				queue.add(out);
				return true;
			}
			
			return false;
		}
	}
}
