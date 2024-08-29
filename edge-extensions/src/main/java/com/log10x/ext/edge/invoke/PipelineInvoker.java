package com.log10x.ext.edge.invoke;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

public class PipelineInvoker {

	private static final String LAUNCHER = "com.log10x.l1x.pipeline.cli.picoli.PipelinePicocliLauncher";
	private static final String FACTORY = "com.log10x.l1x.pipeline.interfaces.launch.EventPipelineFactory";
	private static final String ENV = "com.log10x.l1x.pipeline.interfaces.env.PipelineEnvironment";

	public static void main(String pipelineFactoryClassName, Object parentEnvironment,
			ClassLoader pipelineLoader, String[] args, boolean exitOnFailure) throws ReflectiveOperationException {

		main(pipelineFactoryClassName, parentEnvironment, pipelineLoader, args, exitOnFailure, false);
	}
	
	public static void main(String pipelineFactoryClassName, Object parentEnvironment,
			ClassLoader pipelineLoader, String[] args, boolean exitOnFailure, boolean resetStaticResources) throws ReflectiveOperationException {

		main(pipelineFactoryClassName, parentEnvironment, pipelineLoader, args, exitOnFailure, resetStaticResources, LAUNCHER);
	}

	public static void main(String pipelineFactoryClassName, Object parentEnvironment,
			ClassLoader pipelineLoader, String[] args, boolean exitOnFailure,
			boolean resetStaticResources, String pipelineLauncherClass) throws ReflectiveOperationException {

		Class<?> pipelineFactoryClass = Class.forName(pipelineFactoryClassName, true, pipelineLoader);
		Field instanceField = pipelineFactoryClass.getDeclaredField("Instance");
		Object pipelineFactoryInstance = instanceField.get(null);

		Class<?> launcherClass = Class.forName(pipelineLauncherClass, true, pipelineLoader);

		Class<?> factoryInterface = Class.forName(FACTORY, true, pipelineLoader);
		Class<?> envInterface = Class.forName(ENV, true, pipelineLoader);

		Method main = launcherClass.getMethod("main", factoryInterface, envInterface,
				ClassLoader.class, String[].class, boolean.class, boolean.class);

		main.invoke(null, pipelineFactoryInstance, parentEnvironment, pipelineLoader, args, exitOnFailure, resetStaticResources);
	}
	
	public static Object createEnvironment(String pipelineFactoryClassName, ClassLoader pipelineLoader,
			Map<String, Object> config) throws ReflectiveOperationException {
		
		Class<?> pipelineFactoryClass = Class.forName(pipelineFactoryClassName, true, pipelineLoader);
		Field instanceField = pipelineFactoryClass.getDeclaredField("Instance");
		Object pipelineFactoryInstance = instanceField.get(null);
		
		Class<?> envInterface = Class.forName(ENV, true, pipelineLoader);
		
		Method createEnvironment = pipelineFactoryClass.getMethod("createEnvironment", Map.class, envInterface);
		
		return createEnvironment.invoke(pipelineFactoryInstance, config, null);
	}

	public static Object flushEnvironment(Object environment, ClassLoader pipelineLoader)
			throws ReflectiveOperationException {

		Class<?> envInterface = Class.forName(ENV, true, pipelineLoader);

		Method flushEnvironment = envInterface.getMethod("flushMetrics");

		return flushEnvironment.invoke(environment);
	}
}
