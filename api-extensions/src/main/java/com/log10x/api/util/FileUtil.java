package com.log10x.api.util;

import java.io.Closeable;
import java.io.Flushable;

public class FileUtil {
	public static boolean close(Closeable c) {

		if (c == null) {
			return true;
		}

		try {
			c.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public static boolean flush(Flushable f) {

		if (f == null) {
			return true;
		}

		try {
			f.flush();
			return true;
		} catch (Exception e) {
			return false;
		}
	}
}
