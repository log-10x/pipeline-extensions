package com.log10x.ext.cloud.index;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

import com.log10x.ext.cloud.index.interfaces.ObjectStorageAccessOptions;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.edge.bean.EvaluatorBean;

public class IndexAccessorUtil {

	/**
	 * name of l1x env var containing Cloud access class name.
	 * To learn more, see: {@link http://doc.log10x.com/run/input/objectStorage} 
	 */
	public static final String OBJECT_STORAGE_ACCESS_CLASS_ENV = "objectStorageClassName";
	
	public static final char ACCESS_ALIAS_SEPRATOR = ':';

	public static ObjectStorageIndexAccessor createIndexAccessor(EvaluatorBean evaluator,
		ObjectStorageAccessOptions options) {
			
		String accessorAlias = options.accessorAlias();
		
		if (accessorAlias == null) {
			throw new IllegalStateException("missing accessor alias");
		}
		
		Object accessClassesEnv =  evaluator.env(OBJECT_STORAGE_ACCESS_CLASS_ENV);
		
		if (!(accessClassesEnv instanceof List<?>)) {		
			throw new IllegalStateException("no value supplied for:" + OBJECT_STORAGE_ACCESS_CLASS_ENV);
		}
		
		String className = null;
		
		@SuppressWarnings("unchecked")
		List<Object> accessClasses = (List<Object>)accessClassesEnv;
		
		for (Object accessClass : accessClasses) {
			
			String accessClassStr = String.valueOf(accessClass);
			
			int index = accessClassStr.indexOf(ACCESS_ALIAS_SEPRATOR);
			
			if (index == -1) {
				throw new IllegalStateException("missing '" + ACCESS_ALIAS_SEPRATOR + "' in: " + accessClassStr);
			}
			
			String prefix = accessClassStr.substring(0, index);
			
			if ((index < accessClassStr.length() - 1) &&
				(prefix.equals(accessorAlias))) {
				
				className = accessClassStr.substring(index + 1);
				break;
			}
		}
		
		if (className == null) {
			
			throw new IllegalStateException("could not find index accessor class for: " + 
					accessorAlias + " in: " + accessClasses);
		}

		try {

			Class<?> clazz = Class.forName(className, true, options.getClass().getClassLoader());

			Constructor<?>[] ctors = clazz.getDeclaredConstructors();

			Constructor<?> noParamsCtor = null;
			Constructor<?> optionsParamCtor = null;
			Constructor<?> beanParamCtor = null;
			Constructor<?> twoParamsCtor = null;

			for (Constructor<?> ctor: ctors) {
				
				Class<?>[] params = ctor.getParameterTypes();
				
				switch (params.length) {
				
					case 0: {
						
						noParamsCtor = ctor;
						continue;
					}
				
					case 1: {
						
						if (params[0] == ObjectStorageAccessOptions.class) {		
							optionsParamCtor = ctor;
						} else if (params[0] == EvaluatorBean.class) {
							beanParamCtor = ctor;
						}
						
						continue;
					}
				
					case 2: {
		
						if ((params[1] == EvaluatorBean.class) &&
							(params[0] == ObjectStorageAccessOptions.class)) {
									
							twoParamsCtor = ctor;
						}
							
						continue;					
					}
				}	
			}

			if (twoParamsCtor != null) {

				return (ObjectStorageIndexAccessor) twoParamsCtor.newInstance(options, evaluator);
			}

			if (beanParamCtor != null) {

				return (ObjectStorageIndexAccessor) beanParamCtor.newInstance(evaluator);
			}

			if (optionsParamCtor != null) {

				return (ObjectStorageIndexAccessor) optionsParamCtor.newInstance(options);
			}

			if (noParamsCtor != null) {

				return (ObjectStorageIndexAccessor) noParamsCtor.newInstance();
			}
			
			throw new IllegalStateException("class: " + clazz + 
				" must declare either a parameterless () / (" +
				ObjectStorageAccessOptions.class.getName() + ") / (" + 
				ObjectStorageAccessOptions.class.getName() + ", " + EvaluatorBean.class.getName() +
				") constructor. Found: " + Arrays.asList(ctors));

		} catch (Exception e) {
			throw new IllegalStateException("error instantiating: " + className, e);
		}
	}
}
