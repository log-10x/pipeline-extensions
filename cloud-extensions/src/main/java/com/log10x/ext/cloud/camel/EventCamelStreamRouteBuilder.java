package com.log10x.ext.cloud.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ExtendedCamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.Resource;
import org.apache.camel.spi.RoutesLoader;
import org.apache.camel.support.ResourceHelper;

import com.log10x.ext.cloud.camel.util.stream.OpenPipedOutputStream;
import com.log10x.ext.edge.bean.EvaluatorBean;

/**
 * A Camel route builder used to load the route from a yaml document 
 * and bind the l1x input's evaluator to the route as an 'l1x' bean.
 */
public class EventCamelStreamRouteBuilder extends RouteBuilder {

	public static final String BEAN_NAME = "l1x";

	private final String routeName;
	
	private final String routeContent;
		
	private final EvaluatorBean bean;
 
	protected EventCamelStreamRouteBuilder(String routeName, String routeContent,
		OpenPipedOutputStream pipedOutputStream, EvaluatorBean bean) {
		
		this.routeName = routeName;
		this.routeContent = routeContent;
		this.bean = new EventCamelEvalBean(bean, pipedOutputStream);		
	}

	@Override
	public void configure() throws Exception {

		CamelContext context = this.getContext();

		ExtendedCamelContext extendedCamelContext = context.adapt(ExtendedCamelContext.class);
		RoutesLoader loader = extendedCamelContext.getRoutesLoader();

		Resource resource = ResourceHelper.fromString(this.routeName + ".yaml",
			this.routeContent);

		context.getRegistry().bind(BEAN_NAME, this.bean);

		loader.loadRoutes(resource);
	}
}
