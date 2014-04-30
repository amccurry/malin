package org.apache.hadoop.malin.io.bloom;

public class PackedBloomFilterBitSetLoading extends BloomFilterBitSet {

  private static final int SHIFT = 3;
  private static final int MASK = (1 << 3) - 1;
  private final int _bloomFilterIndex;
  private final byte[] _bits;

  public static void main(String[] args) {
    PackedBloomFilterBitSetLoading bs = new PackedBloomFilterBitSetLoading(0, 100);
    for (int i = 0; i < 100; i += 3) {
      bs.set(i);
    }
    for (int i = 0; i < 100; i += 3) {
      if (bs.get(i)) {
        System.out.println(i);
      }
    }
  }

  public PackedBloomFilterBitSetLoading(int bloomFilterIndex, int numberOfBits) {
    _bloomFilterIndex = bloomFilterIndex;
    _bits = new byte[(numberOfBits / 8) + 1];
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
    int wordNum = (int) (index >> SHIFT);
    int bit = (int) index & MASK;
    int bitmask = 1 << bit;
    _bits[wordNum] |= bitmask;
  }

  public int getBloomFilterIndex() {
    return _bloomFilterIndex;
  }

  public byte[] getBits() {
    return _bits;
  }

}
