package org.apache.hadoop.malin.io.bloom;

/**
 * This is a simple implementation of a bloom filter, it uses a chain of murmur
 * hashes to create the bloom filter.
 * 
 */
public class BloomFilter<T> extends BloomFilterFormulas {

  private static final int SEED = 1;

  private final BloomFilterBitSet _bitSet;
  private final ToBytes<T> _toBytes;
  private final long _numberOfBitsDivBy2;
  private final int _hashes;

  /**
   * Creates a bloom filter with the provided number of hashed and hits.
   * 
   * @param hashes
   *          the hashes to be performed.
   * @param numberOfBits
   *          the numberOfBits to be used in the bit set.
   * @param threadSafe
   *          indicates whether the underlying bit set has to be thread safe or
   *          not.
   */
  public BloomFilter(int hashes, long numberOfBits, ToBytes<T> toBytes,
      BloomFilterBitSet bitSet) {
    _hashes = hashes;
    _numberOfBitsDivBy2 = numberOfBits / 2;
    _bitSet = bitSet;
    _toBytes = toBytes;
  }

  /**
   * Creates a bloom filter with the provided number of hashed and hits.
   * 
   * @param hashes
   *          the hashes to be performed.
   * @param numberOfBits
   *          the numberOfBits to be used in the bit set.
   * @param threadSafe
   *          indicates whether the underlying bit set has to be thread safe or
   *          not.
   */
  public BloomFilter(int hashes, long numberOfBits, ToBytes<T> toBytes,
      boolean threadSafe) {
    this(hashes, numberOfBits, toBytes, threadSafe ? new ThreadSafeBitSet(
        numberOfBits) : new ThreadUnsafeBitSet(numberOfBits));
  }

  /**
   * Creates a bloom filter with the provided number of hashed and hits.
   * 
   * @param probabilityOfFalsePositives
   *          the probability of false positives for the given number of
   *          elements.
   * @param numberOfBits
   *          the numberOfBits to be used in the bit set.
   * @param threadSafe
   *          indicates whether the underlying bit set has to be thread safe or
   *          not.
   */
  public BloomFilter(double probabilityOfFalsePositives, long elementSize,
      ToBytes<T> toBytes, boolean threadSafe) {
    this(getOptimalNumberOfHashesByBits(elementSize,
        getNumberOfBits(probabilityOfFalsePositives, elementSize)),
        getNumberOfBits(probabilityOfFalsePositives, elementSize), toBytes,
        threadSafe);
  }

  /**
   * Creates a thread safe bloom filter with the provided number of hashed and
   * hits.
   * 
   * @param hashes
   *          the hashes to be performed.
   * @param numberOfBits
   *          the numberOfBits to be used in the bit set.
   */
  public BloomFilter(int hashes, long numberOfBits, ToBytes<T> toBytes) {
    this(hashes, numberOfBits, toBytes, true);
  }

  /**
   * Creates a thread safe bloom filter with the provided number of hashed and
   * hits.
   * 
   * @param probabilityOfFalsePositives
   *          the probability of false positives for the given number of
   *          elements.
   * @param numberOfBits
   *          the numberOfBits to be used in the bit set.
   */
  public BloomFilter(double probabilityOfFalsePositives, long elementSize,
      ToBytes<T> toBytes) {
    this(probabilityOfFalsePositives, elementSize, toBytes, true);
  }

  /**
   * Add a key to the bloom filter.
   * 
   * @param key
   *          the key.
   */
  public void add(T key) {
    byte[] bs = _toBytes.toBytes(key);
    addInternal(bs);
  }

  /**
   * Tests a key in the bloom filter, it may provide false positives.
   * 
   * @param key
   *          the key.
   * @return boolean.
   */
  public boolean test(T key) {
    byte[] bs = _toBytes.toBytes(key);
    return testInternal(bs);
  }

  /**
   * Add a key to the bloom filter.
   * 
   * @param key
   *          the key.
   */
  public void addBytes(byte[] key, int offset, int length) {
    byte[] bs = key;
    for (int i = 0; i < _hashes; i++) {
      int hash = MurmurHash.hash(SEED, bs, offset, length);
      setBitSet(hash);
      bs[0]++;
    }
    bs[0] -= _hashes; // reset to original value
  }

  /**
   * Tests a key in the bloom filter, it may provide false positives.
   * 
   * @param key
   *          the key.
   * @return boolean.
   */
  public boolean testBytes(byte[] key, int offset, int length) {
    byte[] bs = key;
    for (int i = 0; i < _hashes; i++) {
      int hash = MurmurHash.hash(SEED, bs, offset, length);
      if (!testBitSet(hash)) {
        bs[0] -= i; // reset to original value
        return false;
      }
      bs[0]++;
    }
    bs[0] -= _hashes; // reset to original value
    return true;
  }

  /**
   * Test the key against the bit set with the proper number of hashes.
   * 
   * @param key
   *          the key.
   * @return boolean.
   */
  private boolean testInternal(byte[] key) {
    byte[] bs = key;
    for (int i = 0; i < _hashes; i++) {
      int hash = MurmurHash.hash(SEED, bs, bs.length);
      if (!testBitSet(hash)) {
        bs[0] -= i; // reset to original value
        return false;
      }
      bs[0]++;
    }
    bs[0] -= _hashes; // reset to original value
    return true;
  }

  /**
   * Adds the key to the bit set with the proper number of hashes.
   * 
   * @param key
   *          the key.
   */
  private void addInternal(byte[] key) {
    byte[] bs = key;
    for (int i = 0; i < _hashes; i++) {
      int hash = MurmurHash.hash(SEED, bs, bs.length);
      setBitSet(hash);
      bs[0]++;
    }
    bs[0] -= _hashes; // reset to original value
  }

  /**
   * Sets the bit position in the bit set.
   * 
   * @param hash
   *          the hash produced by the murmur class.
   */
  private void setBitSet(int hash) {
    _bitSet.set(getIndex(hash));
  }

  /**
   * Tests the bit position in the bit set.
   * 
   * @param hash
   *          the hash produced by the murmur class.
   * @return boolean.
   */
  private boolean testBitSet(int hash) {
    return _bitSet.get(getIndex(hash));
  }

  /**
   * Gets the index into the bit set for the given hash.
   * 
   * @param hash
   *          the hash produced by the murmur class.
   * @return the index position.
   */
  private long getIndex(int hash) {
    return (hash % _numberOfBitsDivBy2) + _numberOfBitsDivBy2;
  }

}
