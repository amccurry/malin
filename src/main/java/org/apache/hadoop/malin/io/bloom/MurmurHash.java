package org.apache.hadoop.malin.io.bloom;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * 
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public class MurmurHash {
  public static int hash(int seed, byte[] data, int offset, int length) {
    if (offset == 0) {
      return hash(seed, data, length);
    }
    int m = 0x5bd1e995;
    int r = 24;
    int h = seed ^ length;
    int len = length;
    int len_4 = len >> 2;
    for (int i = 0; i < len_4; i++) {
      int i_4 = i << 2;
      int offset_i_4 = offset + i_4;
      int k = data[offset_i_4 + 3];
      k = k << 8;
      k = k | (data[offset_i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (data[offset_i_4 + 1] & 0xff);
      k = k << 8;
      k = k | (data[offset_i_4 + 0] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }
    int len_m = len_4 << 2;
    int left = len - len_m;
    if (left != 0) {
      int offset_len = len + offset;
      if (left >= 3) {
        h ^= (int) data[offset_len - 3] << 16;
      }
      if (left >= 2) {
        h ^= (int) data[offset_len - 2] << 8;
      }
      if (left >= 1) {
        h ^= (int) data[offset_len - 1];
      }
      h *= m;
    }
    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;
    return h;
  }

  public static int hash(int seed, byte[] data, int length) {
    int m = 0x5bd1e995;
    int r = 24;
    int h = seed ^ length;
    int len = length;
    int len_4 = len >> 2;
    for (int i = 0; i < len_4; i++) {
      int i_4 = i << 2;
      int k = data[i_4 + 3];
      k = k << 8;
      k = k | (data[i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 1] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 0] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }
    int len_m = len_4 << 2;
    int left = len - len_m;
    if (left != 0) {
      if (left >= 3) {
        h ^= (int) data[len - 3] << 16;
      }
      if (left >= 2) {
        h ^= (int) data[len - 2] << 8;
      }
      if (left >= 1) {
        h ^= (int) data[len - 1];
      }
      h *= m;
    }
    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;
    return h;
  }
}