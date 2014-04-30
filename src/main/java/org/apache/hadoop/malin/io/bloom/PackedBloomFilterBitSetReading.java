package org.apache.hadoop.malin.io.bloom;

public class PackedBloomFilterBitSetReading extends BloomFilterBitSet {

  private static final int SHIFT = 3;
  private static final int MASK = (1 << 3) - 1;
  private final int _bloomFilterIndex;
  private final byte[] _bits;

  public PackedBloomFilterBitSetReading(int bloomFilterIndex, byte[] bits) {
    _bloomFilterIndex = bloomFilterIndex;
    _bits = bits;
  }

  @Override
  public boolean get(long index) {
    
    
    
    int i = (int) (index >> SHIFT); // divide by 8
    int bit = (int) index & MASK; // mask by 4
    int bitmask = 1 << bit;
    return (_bits[i] & bitmask) != 0;
  }

  @Override
  public void set(long index) {
    throw new RuntimeException("not supported");
  }

}
