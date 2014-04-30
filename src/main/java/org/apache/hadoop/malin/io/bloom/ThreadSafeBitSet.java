package org.apache.hadoop.malin.io.bloom;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A thread safe implementation of a bit set. Uses {@link AtomicLongArray} for
 * thread safety, by using compare and swap.
 */
public class ThreadSafeBitSet extends BloomFilterBitSet implements Cloneable {

  private AtomicLongArray _bits;

  public ThreadSafeBitSet(long numBits) {
    _bits = new AtomicLongArray(ThreadUnsafeBitSet.bits2words(numBits));
  }

  public ThreadSafeBitSet(long[] bits) {
    _bits = new AtomicLongArray(bits);
  }

  @Override
  public boolean get(long index) {
    int i = (int) (index >> 6);
    int bit = (int) index & 0x3f;
    long bitmask = 1L << bit;
    long currentWord = _bits.get(i);
    return (currentWord & bitmask) != 0;
  }

  @Override
  public void set(long index) {
    int wordNum = (int) (index >> 6);
    int bit = (int) index & 0x3f;
    long bitmask = 1L << bit;
    long currentWord = _bits.get(wordNum);
    long newWord = currentWord | bitmask;
    while (!_bits.compareAndSet(wordNum, currentWord, newWord)) {
      currentWord = _bits.get(wordNum);
      newWord = currentWord | bitmask;
    }
  }

}
