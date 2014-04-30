package org.apache.hadoop.malin.io.bloom;


/**
 * Normal bit set that uses long as words.
 */
public class ThreadUnsafeBitSet extends BloomFilterBitSet implements Cloneable {

	private long[] _bits;

	public ThreadUnsafeBitSet(long numBits) {
		_bits = new long[bits2words(numBits)];
	}

	public ThreadUnsafeBitSet(long[] bits) {
		_bits = bits;
	}

	@Override
	public boolean get(long index) {
		int i = (int) (index >> 6);
		int bit = (int) index & 0x3f;
		long bitmask = 1L << bit;
		return (_bits[i] & bitmask) != 0;
	}

	@Override
	public void set(long index) {
		int wordNum = (int) (index >> 6);
		int bit = (int) index & 0x3f;
		long bitmask = 1L << bit;
		_bits[wordNum] |= bitmask;
	}
	
	public static int bits2words(long numBits) {
		return (int) (((numBits - 1) >>> 6) + 1);
	}

}
