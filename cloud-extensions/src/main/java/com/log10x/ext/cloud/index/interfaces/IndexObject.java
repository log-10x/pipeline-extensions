package com.log10x.ext.cloud.index.interfaces;

import java.util.Map;
import java.util.Objects;

/**
 * Defines an object stored during the indexing of an input object within an
 * underlying KV storage (e.g. AWS S3). Index objects include JSON serialized
 * l1x template values and encoded bloom filters.
 * 
 * To learn more see http://doc.log10x.com/run/input/objectStorage/index/#index-objects 
 */
public class IndexObject {

	/**
	 * the key value of this index object
	 */
	public final String name;

	/**
	 * optional metadata tags
	 */
	public final Map<String, String> tags;

	public IndexObject(String name, Map<String, String> tags) {
		this.name = name;
		this.tags = tags;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof IndexObject)) {
			return false;
		}

		if (this == obj) {
			return true;
		}

		IndexObject other = (IndexObject) obj;

		return ((Objects.equals(this.name, other.name)) &&
				(tags.equals(other.tags)));
	}

	@Override
	public String toString() {
		return String.format("%s tags: %s", name, tags);
	}
}
