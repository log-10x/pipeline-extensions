package com.log10x.ext.cloud.camel;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import org.apache.camel.Handler;

import com.log10x.ext.cloud.camel.util.stream.OpenPipedOutputStream;
import com.log10x.ext.edge.bean.EvaluatorBean;

/**
 * A decorator for an l1x evaluator made accessible to a Camel route as a bean
 * object. See: {@link https://camel.apache.org/manual/bean-binding.html}
 */
public class EventCamelEvalBean implements EvaluatorBean {

	private final OpenPipedOutputStream outputStream;

	private final EvaluatorBean evaluator;

	public EventCamelEvalBean(EvaluatorBean evaluator, OpenPipedOutputStream outputStream) {

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
	public Object dict(String key) {
		return evaluator.dict(key);
	}

	@Override
	public Object dict(String key, Object value) {
		return evaluator.dict(key, value);
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
	public Object env(String key) {
		return evaluator.env(key);
	}

	@Override
	public void debugState(String key, Object value) {
		evaluator.debugState(key, value);
	}

	public void closeRoute() {
		try {
			this.outputStream.terminate();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
