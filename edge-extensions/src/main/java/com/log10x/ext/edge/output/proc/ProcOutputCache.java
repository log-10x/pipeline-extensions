package com.log10x.ext.edge.output.proc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import com.log10x.api.eval.EvaluatorBean;

public class ProcOutputCache {
	public static final ProcOutputCache Instance = new ProcOutputCache();
	
	private final Map<String, LinkedList<ProcOutputStreamImpl>> cache = new HashMap<>(4);
	
	synchronized ProcOutputStreamImpl get(
			ProcStreamOptions options, EvaluatorBean evaluatorBean) {
		
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
					curr.options.args(), 
					options.args())) {
					
					continue;
				}
				
				if (!Objects.equals(
					curr.options.command(), 
					options.command())) {
					
					continue;
				}
				
				if (!options.allowMultipleStreams()) {
					iter.remove();
				}
				
				return curr;	
			}
		}
		
		ProcOutputStreamImpl result = ProcOutputStreamImpl.newImpl(options, evaluatorBean);
		
		put(result);
		
		return result;
	}

	synchronized boolean put(ProcOutputStreamImpl out){
		
		if (!out.isAlive()) {
			return false;
		}
		
		String key = out.options.command();
		
		LinkedList<ProcOutputStreamImpl> queue = cache.get(key);
		
		if (queue == null) {
			queue = new LinkedList<>();
			cache.put(key, queue);
		}
		
		if (queue.contains(out)) {
			return true;
		}
		
		if (queue.size() < out.options.maxCacheSize()) {	
			
			queue.add(out);
			return true;
		}
		
		return false;
	}
	
	public synchronized void close() {
		
		for (LinkedList<ProcOutputStreamImpl> queue : cache.values()) {
			
			for (ProcOutputStreamImpl out : queue) {
				
				if (out.isAlive()) {
					
					try {
						out.close();
					} catch (IOException e) {
					}
				}
			}
		}
		
		this.cache.clear();
	}
}
