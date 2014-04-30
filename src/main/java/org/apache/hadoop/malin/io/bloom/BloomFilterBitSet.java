package org.apache.hadoop.malin.io.bloom;


public abstract class BloomFilterBitSet {

  public abstract boolean get(long index);

  public abstract void set(long index);

}
