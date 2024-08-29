package com.log10x.ext.cloud.camel;

import java.io.OutputStream;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for Apache Camel route processors.
 * 
 * @param <T> the OutputStream type used to bind the route to its l1x pipeline input
 */
public abstract class CamelStreamProcessor<T extends OutputStream> extends BaseRouteProcessor {

	private static final Logger logger = LogManager.getLogger(CamelStreamProcessor.class);

	protected T destinationStream;

	protected abstract String name();

	protected abstract boolean shouldReplaceDestination(OutputStream original);

	protected abstract T createDestinationStream(Exchange exchange, OutputStream outputStream);

	@Override
	public void process(Exchange exchange) throws Exception {

		Object streamObj = routeProp(exchange, STREAM);

		if (!(streamObj instanceof OutputStream)) {

			logger.error("could not set {} stream: empty 'stream' header", name());
			return;
		}

		OutputStream outputStream = (OutputStream) streamObj;

		if ((this.destinationStream == null) ||
			(shouldReplaceDestination(outputStream))) {

			this.destinationStream = createDestinationStream(exchange, outputStream);

			if (this.destinationStream == null) {

				logger.error("could not set {} stream: failed creating.", name());
				return;
			}
		}

		Message message = exchange.getMessage();

		message.setHeader(STREAM, this.destinationStream);
	}
}
