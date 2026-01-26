package com.log10x.ext.cloud.index.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorUtil {

	public static boolean safeAwaitTermination(ExecutorService executorService) {

		return safeAwaitTermination(executorService, 1, TimeUnit.MINUTES);
	}

	public static boolean safeAwaitTermination(ExecutorService executorService, long timeout, TimeUnit unit) {

		try {

			executorService.shutdown();

			return executorService.awaitTermination(timeout, unit);

		} catch (InterruptedException e) {
			return false;
		}
	}
}
