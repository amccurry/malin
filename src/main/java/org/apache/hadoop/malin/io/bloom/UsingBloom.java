package org.apache.hadoop.malin.io.bloom;

import java.util.BitSet;
import java.util.UUID;

public class UsingBloom {

  @SuppressWarnings("serial")
  public static void main(String[] args) {

    long elementSize = 100000;
    double probabilityOfFalsePositives = 0.0064872;

    int numberOfBytes = BloomFilterFormulas.getNumberOfBytes(probabilityOfFalsePositives, elementSize);
    int numberOfBits = BloomFilterFormulas.getNumberOfBits(probabilityOfFalsePositives, elementSize);
    int optimalNumberOfHashesByBits = BloomFilterFormulas.getOptimalNumberOfHashesByBits(elementSize, numberOfBits);
    System.out.println(numberOfBytes);
    System.out.println(2 * 2 * 2 * 2 * 2 * 1024 * 4);

    ToBytes<String> toBytes = new ToBytes<String>() {
      @Override
      public byte[] toBytes(String key) {
        return key.getBytes();
      }
    };

    PackedBloomFilterBitSetLoading[] bitSets = new PackedBloomFilterBitSetLoading[7];
    for (int i = 0; i < bitSets.length; i++) {
      PackedBloomFilterBitSetLoading bitSet = new PackedBloomFilterBitSetLoading(i, numberOfBits);
      bitSets[i] = bitSet;
      BloomFilter<String> bloomFilter = new BloomFilter<>(optimalNumberOfHashesByBits, numberOfBits, toBytes, bitSet);
      for (int n = 0; n < elementSize; n++) {
        bloomFilter.add(UUID.randomUUID().toString());
      }
    }

    PackedBloomFilterBitSetLoading result = new PackedBloomFilterBitSetLoading(-1,numberOfBits * bitSets.length);
    int position = 0;
    for (int o = 0; o < numberOfBits; o++) {
      for (int i = 0; i < bitSets.length; i++) {
        PackedBloomFilterBitSetLoading reading = bitSets[i];
        if (reading.get(o)) {
          result.set(position);
        }
        position++;
      }
    }
    
    
    
    
    
  }

}
