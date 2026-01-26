package com.log10x.api.pipeline.launch;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;

/**
 * This interface provides a mechanism to enable different types of 
 * containers (web servers, lambda functions, CLI programs) to launch
 * 10x pipelines programmatically. 
 */
public interface PipelineFactory extends Closeable, Flushable {

	public enum PipelineFactoryType {			
		Cloud,
		Edge
	}
	
	
	/**
	 * Launches an 10x pipeline
	 * 
	 * @param 	pipelineFactoryType specifies the type of pipeline to run: 
	 * 			{@link https://doc.log10x.com/architecture/flavors/#cloud|cloud} or
	 * 			{@link https://doc.log10x.com/architecture/flavors/#edge|edge}.
	 * @param pipelineLoader	specifies a root class loader for resolving fully qualified
	 * 							class names at run time specified by config files.
	 * 							To learn more see {@link https://doc.log10x.com/run/bootstrap/#jarfiles|jarfiles}.
	 * @param args				Launch arguments for use by the pipeline.
	 * 							To learn more see {@link https://doc.log10x.com/config/cli/|launch arguments}
	 * @param exitOnFailure		Signals the JVM to exit if the pipeline fails to launch
	 * @param supportCustomJars	Set to true to allow dynamic loading of classes via their
	 * 							Fully qualified names at run time. This option
	 * 							is available for the {@link https://doc.log10x.com/architecture/flavors/#jit-edge|jit-edge|
	 * 							and {@link https://doc.log10x.com/architecture/flavors/#cloud|cloud| runtime flavors.
	 */
	public static void launchPipeline(PipelineFactoryType pipelineFactoryType, 
		ClassLoader pipelineLoader, String[] args, boolean exitOnFailure,
		boolean supportCustomJars) {
		
		Singleton.instance().launch(pipelineFactoryType, pipelineLoader,
			args, exitOnFailure, supportCustomJars);	
	}
	
	/**
	 * Invokes the {@link PipelineFactory#launchPipeline} method followed by
	 * a call to {@link #shutdown}. This overload is intended for use
	 * by JVMs that execute a single pipeline as part of their main(String[] args)
	 * entry point method (e.g., Lambda functions, CLI programs)
	 */
	public static void launchSinglePipeline(PipelineFactoryType pipelineFactoryType, 
		ClassLoader pipelineLoader, String[] args, boolean exitOnFailure,
		boolean supportCustomJars) {
			
		try {
			
			launchPipeline(pipelineFactoryType, pipelineLoader,
				args, exitOnFailure, supportCustomJars);	
			
		} finally {
			
			shutdown();
		}
	}
	
	/**
	 * Flush health/debug metrics from executing pipelines to the 10x Prometheus console
	 * To learn more see {@link https://doc.log10x.com/run/output/metric/log10x/|10x Console}
	 */
	public static void flushMetrics() {
		
		if (Singleton.instance != null) {
			
			try {
				
				Singleton.instance.flush();
				
			} catch (IOException e) {
				throw new UncheckedIOException("error closing pipeline", e);	
			}
		}
	}

	/**
	 * Release global caches used across calls to the {@link #launch} method.
	 * This method enables web server containers that launch multiple pipelines
	 * within a host JVM to clear global resources (e.g., thread polls, static caches)
	 * to allow for an orderly shutdown.
	 */
	public static void shutdown() {
		
		if (Singleton.instance != null) {
			
			try {
				
				synchronized (Singleton.instance) {
					
					Singleton.instance.close();
					Singleton.instance = null;				
				}
				
			} catch (IOException e) {
				throw new UncheckedIOException("error closing pipeline factory", e);	
			}
		}
	}
	
	/**
	 * This method is implemented by the 10x engine. 
	 * To learn more see {@link #launchPipeline}
	 */
	public void launch(PipelineFactoryType pipelineFactoryType, 
		ClassLoader pipelineLoader, String[] args, boolean exitOnFailure,
		boolean supportCustomJars);
	
	
	/**
	 * Singleton helper
	 */
	static class Singleton {	
		
		protected static PipelineFactory instance;
		
		protected static PipelineFactory instance() {
			
			if (instance != null) {
				return instance;
			}
			
			synchronized (Singleton.class) {
				
				if (instance != null) {
					return instance;
				}
				
				try {
					
					Class<?> pipelineFactoryClass = Class.forName("com.log10x.eng.pipeline.env.PipelineEnvironmentFactory");
										
					Method createFactoryMethod = pipelineFactoryClass.getMethod("create");
						
					instance = (PipelineFactory)createFactoryMethod.invoke(null);
									
				} catch (Exception e) {
					throw new IllegalStateException("could not initialize pipeline factory", e);
				}
			}
			
			return instance;	
		}
	}
	
	/**
	 * A convenience method to force the factory to instantiate for use by launchers
	 * where there is a delay between startup and pipeline execution (e.g., web servers).
	 */
	public static void initialize() {
		Singleton.instance();
	}
}
