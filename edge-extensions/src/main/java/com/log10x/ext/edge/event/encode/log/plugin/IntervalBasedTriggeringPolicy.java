package com.log10x.ext.edge.event.encode.log.plugin;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.rolling.AbstractTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

/**
 * A log4j2 trigger policy plugin that rolls a file over based on intervals.
 * Used by: {@link https://github.com/log-10x/config/blob/main/pipelines/run/modules/input/forwarder/filebeat/input/log4j2.yaml}
 */
@Plugin(name = "IntervalBasedTriggeringPolicy", category = Core.CATEGORY_NAME, printObject = true)
public final class IntervalBasedTriggeringPolicy extends AbstractTriggeringPolicy {

	private static final long NO_ROLLOVER = -1;
	
	private final long intervalMs;
	private final long maxRandomDelayMillis;

	private long nextRolloverMillis;

	private RollingFileManager manager;

	private IntervalBasedTriggeringPolicy(final long intervalMs, final long maxRandomDelayMillis) {
		this.intervalMs = intervalMs;
		this.maxRandomDelayMillis = maxRandomDelayMillis;
	}

	public long getIntervalMs() {
		return intervalMs;
	}

	public long getNextRolloverMillis() {
		return nextRolloverMillis;
	}

	@Override
	public void initialize(final RollingFileManager aManager) {
		this.manager = aManager;

		nextRolloverMillis = calcNextRolloverMillis();
	}

	/**
	 * Determines whether a rollover should occur.
	 * 
	 * @param event A reference to the currently event.
	 * @return true if a rollover should occur.
	 */
	@Override
	public boolean isTriggeringEvent(final LogEvent event) {
		if (intervalMs <= 0) {
			return false;
		}

		final long nowMillis = event.getTimeMillis();

		if (nowMillis >= nextRolloverMillis) {
			long currentTimeMillis = System.currentTimeMillis();
			nextRolloverMillis = calcNextRolloverMillis(currentTimeMillis);

			manager.getPatternProcessor().setCurrentFileTime(currentTimeMillis);

			return true;
		}

		return false;
	}

	private long calcNextRolloverMillis() {
		return calcNextRolloverMillis(System.currentTimeMillis());
	}

	private long calcNextRolloverMillis(long currentTimeMillis) {
		return ThreadLocalRandom.current().nextLong(0, 1 + maxRandomDelayMillis) + intervalMs + currentTimeMillis;
	}

	@Override
	public String toString() {
		return "IntervalBasedTriggeringPolicy(nextRolloverMillis=" + nextRolloverMillis + ", intervalMs=" + intervalMs
				+ ")";
	}

	@PluginBuilderFactory
	public static IntervalBasedTriggeringPolicy.Builder newBuilder() {
		return new Builder();
	}

	public static class Builder implements org.apache.logging.log4j.core.util.Builder<IntervalBasedTriggeringPolicy> {

		@PluginBuilderAttribute
		private String interval = null;

		@PluginBuilderAttribute
		private int maxRandomDelay = 0;

		@Override
		public IntervalBasedTriggeringPolicy build() {
			final long intervalMs = calcIntervalMs();
			final long maxRandomDelayMillis = TimeUnit.SECONDS.toMillis(maxRandomDelay);

			return new IntervalBasedTriggeringPolicy(intervalMs, maxRandomDelayMillis);
		}

		private long calcIntervalMs() {
			if (interval == null) {
				return NO_ROLLOVER;
			}
			
			return DurationStyle.SIMPLE.parse(interval).toMillis();
		}

		public String getInterval() {
			return interval;
		}

		public int getMaxRandomDelay() {
			return maxRandomDelay;
		}

		public Builder withInterval(final String interval) {
			this.interval = interval;
			return this;
		}

		public Builder withMaxRandomDelay(final int maxRandomDelay) {
			this.maxRandomDelay = maxRandomDelay;
			return this;
		}
	}
}