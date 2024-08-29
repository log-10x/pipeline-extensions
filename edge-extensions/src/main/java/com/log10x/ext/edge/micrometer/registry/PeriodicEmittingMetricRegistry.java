package com.log10x.ext.edge.micrometer.registry;

import static io.micrometer.core.instrument.config.validate.PropertyValidator.getDuration;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterRegistryConfig;

public class PeriodicEmittingMetricRegistry extends CompositeMeterRegistry implements Runnable, Closeable {

	public interface EmittingMetricRegistry extends Closeable {
		public void emit();
	}

	private static final String STEP = "step";
	private static final Duration DEFAULT_STEP = Duration.ofMinutes(1);

	public interface PeriodicEmittingConfig extends MeterRegistryConfig {
		default Duration emitStep() {
			return getDuration(this, STEP).orElse(DEFAULT_STEP);
		}
	}

	private static final Logger logger = LogManager.getLogger(PeriodicEmittingMetricRegistry.class);

	private final EmittingMetricRegistry internalRegistry;
	private final long interval;
	private final Thread thread;

	private volatile boolean closed;

	PeriodicEmittingMetricRegistry(EmittingMetricRegistry internalRegistry, long interval) {
		super(Clock.SYSTEM, Collections.singleton((MeterRegistry) internalRegistry));

		this.internalRegistry = internalRegistry;
		this.interval = interval;

		this.thread = new Thread(this);
		thread.setName(internalRegistry.getClass().getSimpleName());

		thread.setDaemon(false);
		thread.start();
	}

	@Override
	public void run() {

		boolean first = true;

		while (!this.closed) {

			if (first) {
				first = false;
			} else {
				this.emit();
			}

			synchronized (this.thread) {

				try {
					thread.wait(this.interval);
				} catch (InterruptedException e) {
				}
			}
		}
	}

	private void emit() {
		this.internalRegistry.emit();
	}

	@Override
	public void close() {

		this.closed = true;

		synchronized (this.thread) {
			thread.notifyAll();
		}

		this.emit();

		try {
			this.internalRegistry.close();
		} catch (IOException e) {
			logger.warn("Problem closing internal registry {}.", this.internalRegistry.getClass().getSimpleName(), e);
		}

		super.close();
	}
}
