package com.log10x.ext.cloud.camel;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import org.apache.camel.Handler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.ext.cloud.camel.util.stream.OpenPipedOutputStream;

/**
 * A decorator for an 10x evaluator made accessible to a Camel route as a bean
 * object. See: {@link https://camel.apache.org/manual/bean-binding.html}
 */
public class CamelEvaluatorBean implements EvaluatorBean, Closeable {

	protected static final Logger logger = LogManager.getLogger(CamelEvaluatorBean.class);

	private final OpenPipedOutputStream outputStream;

	private final EvaluatorBean evaluator;
	
	public CamelEvaluatorBean(EvaluatorBean evaluator, 
		OpenPipedOutputStream outputStream) {

		this.evaluator = evaluator;
		this.outputStream = outputStream;
	}

	public OutputStream stream() {
		return this.outputStream;
	}

	@Override
	@Handler
	public Object eval(String exp) {
		return evaluator.eval(exp);
	}
	
	@Override
	public Object get(String key) {
		return evaluator.get(key);
	}

	@Override
	public Object set(String key, Object value) {
		return evaluator.set(key, value);
	}

	@Override
	public boolean truthy(String exp) {
		return evaluator.truthy(exp);
	}

	@Override
	public Object env(String key, Object defaultValue) {
		return evaluator.env(key, defaultValue);
	}

	@Override
	public void debugState(String key, Object value) {
		evaluator.debugState(key, value);
	}

	@Handler
	@Override
	public void close() throws IOException {
		
		logger.info("closing context stream");
		
		try {
			outputStream.terminate();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
