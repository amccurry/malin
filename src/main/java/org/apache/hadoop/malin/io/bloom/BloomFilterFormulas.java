package org.apache.hadoop.malin.io.bloom;

/**
 * Provides some useful methods for estimating memory usage, optimal hashes, etc.
 */
public abstract class BloomFilterFormulas {
	
	/**
	 * k = (m / n) ln 2 from wikipedia. 
	 * @param elementSize the number of elements expected.
	 * @param numberOfBytes the number of bytes allowed.
	 * @return the best number of hashes.
	 */
	public static int getOptimalNumberOfHashesByBytes(long elementSize, long numberOfBytes) {
		return getOptimalNumberOfHashesByBits(elementSize, numberOfBytes *8);
	}
	
	/**
	 * k = (m / n) ln 2 from wikipedia. 
	 * @param elementSize the number of elements expected.
	 * @param numberOfBits the number of bytes allowed.
	 * @return the best number of hashes.
	 */
	public static int getOptimalNumberOfHashesByBits(long elementSize, long numberOfBits) {
		return (int) Math.ceil(Math.log(2) * ((double) numberOfBits / elementSize));
	}
	
	/**
	 * Calculate the number of bytes needed to produce the provided probability of false 
	 * positives with the given element size.
	 * @param probabilityOfFalsePositives the probability of false positives for the given number of elements. 
	 * @param elementSize the estimated element size.
	 * @return the number of bytes.
	 */
	public static int getNumberOfBytes(double probabilityOfFalsePositives, long elementSize) {
		return getNumberOfBits(probabilityOfFalsePositives,elementSize) / 8;
	}
	
	/**
	 * Calculate the number of bits needed to produce the provided probability of false 
	 * positives with the given element size.
	 * @param probabilityOfFalsePositives the probability of false positives. 
	 * @param elementSize the estimated element size.
	 * @return the number of bytes.
	 */
	public static int getNumberOfBits(double probabilityOfFalsePositives, long elementSize) {
		return (int) (Math.abs(elementSize * Math.log(probabilityOfFalsePositives)) / (Math.pow(Math.log(2),2)));
	}
	
}
