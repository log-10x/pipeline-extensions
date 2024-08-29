package com.log10x.ext.cloud.index.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ArgsUtil {

	public static List<String> toList(Map<String, String> map) {
		
		return map.entrySet()
                .stream()
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
	}
	
	public static Map<String, String> toMap(List<String> list) {
		
		return IntStream.range(0, list.size() / 2).boxed()
				.collect(Collectors.toMap(
						i -> list.get(i * 2),
						i -> list.get(i * 2 + 1)));
	}
}
