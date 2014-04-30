package org.apache.hadoop.malin.io.bloom;

import java.io.Serializable;

/**
 * Turns a key into bytes.
 */
public interface ToBytes<T> extends Serializable {
	
	/**
	 * Turn the given keys into bytes.
	 * @param key the key.
	 * @return bytes that represent the key.
	 */
	byte[] toBytes(T key);
}