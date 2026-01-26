package com.log10x.api.util.micrometer;

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
	
	public interface PeriodicEmittingConfig extends MeterRegistryConfig {
		
		public static final Duration DEFAULT_STEP = Duration.ofMinutes(1);
		
		default Duration emitStep() {
			return getDuration(this, STEP).orElse(DEFAULT_STEP);
		}
	}

	private static final Logger logger = LogManager.getLogger(PeriodicEmittingMetricRegistry.class);

	private final EmittingMetricRegistry internalRegistry;
	private final long interval;
	private final Thread thread;

	private volatile boolean closed;

	public PeriodicEmittingMetricRegistry(EmittingMetricRegistry internalRegistry, long interval) {
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
				this.emit(false);
			}

			synchronized (this.thread) {

				try {
					thread.wait(this.interval);
				} catch (InterruptedException e) {
				}
			}
		}
	}

	private synchronized void emit(boolean force) {
		
		if ((this.closed) &&
			(!force)) {

			return;
		}

		this.internalRegistry.emit();
	}

	@Override
	public synchronized void close() {

		if (this.closed) {
			return;
		}

		this.closed = true;

		synchronized (this.thread) {
			thread.notifyAll();
		}

		this.emit(true);

		try {
			internalRegistry.close();
			
		} catch (IOException e) {
			
			logger.warn("error closing internal registry {}.", 
				internalRegistry.getClass().getSimpleName(), e);
		}

		super.close();
	}
}
