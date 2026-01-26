package com.log10x.ext.cloud.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dsl.yaml.YamlRoutesBuilderLoader;
import org.apache.camel.spi.Resource;
import org.apache.camel.support.ResourceHelper;

import com.log10x.api.eval.EvaluatorBean;

/**
 * A Camel route builder used to load the route from a yaml document
 * and bind the 10x input's evaluator to the route as a 'tenx' bean.
 */
public class EventCamelStreamRouteBuilder extends RouteBuilder {

    public static final String BEAN_NAME = "tenx";
    public static final String INSECURE_SSL_BEAN_NAME = "insecureSslContext";

    private final String routeName;
    private final String routeContent;
    private final EvaluatorBean bean;

    public EventCamelStreamRouteBuilder(String routeName, String routeContent, EvaluatorBean bean) {
        this.routeName = routeName;
        this.routeContent = routeContent;
        this.bean = bean;
    }

    @Override
    public void configure() throws Exception {
        CamelContext context = getCamelContext();

        // Bind the EvaluatorBean to the registry
        context.getRegistry().bind(BEAN_NAME, this.bean);

        // Register insecure SSL context for use when SSL verification is disabled
        context.getRegistry().bind(INSECURE_SSL_BEAN_NAME, new InsecureSslContextParameters());

        // Create a Resource from the YAML content
        Resource resource = ResourceHelper.fromString(this.routeName + ".yaml", this.routeContent);

        try (YamlRoutesBuilderLoader loader = new YamlRoutesBuilderLoader()) {
            loader.setCamelContext(context);

            // loadRoutesBuilder returns a RoutesBuilder (not always RouteBuilder)
            var yamlRoutes = loader.loadRoutesBuilder(resource);

            context.addRoutes(yamlRoutes);
        }
    }
}