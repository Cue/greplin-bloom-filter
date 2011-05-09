/*
 * Copyright 2010 Greplin, Inc. All Rights Reserved.
 */

package com.greplin.bloomfilter;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents the metadata associated with a serialized bloom filter.
 *
 * The old header format was:
 * 4 bytes for the number of hash Fns
 * 4 bytes for the 'real size' (data + metadata).
 * That header format always assumed 4-bit buckets
 *
 * The header format is now as follows:
 * 8 bytes of zeroes (to distinguish this format from the old one)
 * 3 bytes for the 'magic word' which is: 0xB1 0xF1 0xCA
 * 1 byte for the header version (currently 2 - implicitly 1 in the old header format)
 * 4 bytes for header length (currently 32 bytes)
 * 4 bytes for real-size (total size of data + metadata)
 * 4 bytes for the number of hash fns
 * 4 bytes for the number of counting-bits in each bucket
 * 4 bytes of 0 padding to make the whole header 32-bytes even
 * The new format has the first two bytes as '0', which the old format will never have - so we can safely identify
 * which is which. If we detect the old format, we can safely assume there are four bits per bucket.
 * This is a little convoluted, but it's the safest way to guarantee backwards compatibility with the old format
 */
class HeaderInformation {

  private static final int INT_SIZE = 32;
  private static final byte VERSION = 2;
  private static final byte[] MAGIC_WORD = {(byte) 0xB1, (byte) 0xF1, (byte) 0xCA};

  private final byte version;
  private final int headerLength;
  private final int totalLength;
  private final int hashFns;
  private final BucketSize bucketSize;

  private HeaderInformation(byte version, int headerLength, int totalLength, int hashFns, BucketSize bucketSize) {
    this.version = version;
    this.headerLength = headerLength;
    this.totalLength = totalLength;
    this.hashFns = hashFns;
    this.bucketSize = bucketSize;
  }

  public static HeaderInformation readHeader(ByteBuffer buffer) throws IOException {
    final int firstInt = buffer.getInt();
    final int secondInt = buffer.getInt();

    if (firstInt == 0 && secondInt == 0) {
      return readNewStyleHeader(buffer);
    } else {
      return readOldStyleHeader(buffer, firstInt, secondInt);
    }
  }

  private static HeaderInformation readOldStyleHeader(ByteBuffer file, int hashFns, int realSize) {
    return new HeaderInformation((byte)1, 2 * INT_SIZE, realSize, hashFns, BucketSize.FOUR);
  }

  private static HeaderInformation readNewStyleHeader(ByteBuffer buffer) throws IOException {

    // verify the magic word is present and intact
    final byte[] shouldBeMagicWord = new byte[MAGIC_WORD.length];
    buffer.get(shouldBeMagicWord);
    if (!Arrays.equals(MAGIC_WORD, shouldBeMagicWord)) {
      throw new InvalidBloomFilter("Invalid Magic Word " + Arrays.toString(shouldBeMagicWord));
    }

    // verify the version is correct
    final byte version = buffer.get();
    if (!(version == VERSION)) {
      throw new InvalidBloomFilter("Unrecognized version (" + version + ")");
    }

    final int headerLen = buffer.getInt();
    if (headerLen < 32) {
      throw new InvalidBloomFilter("Unexpectedly short header length (" + headerLen + " bytes)");
    }

    final int realSize = buffer.getInt();
    if (realSize < headerLen) {
      throw new InvalidBloomFilter("Impossibly short size (" + realSize + " bytes)");
    }

    final int hashFns = buffer.getInt();
    if (hashFns <= 0) {
      throw new InvalidBloomFilter("Invalid number of hashFns (" + hashFns + " bytes)");
    }

    final int bucketSizeInt = buffer.getInt();
    final BucketSize bucketSize = BucketSize.getBucketSize(bucketSizeInt);
    if (bucketSize == null) {
      throw new InvalidBloomFilter("Invalid bucketSize (" + bucketSize + " bytes)");
    }

    if (buffer.getInt() != 0) {
      throw new InvalidBloomFilter("Invalid end padding");
    }

    return new HeaderInformation(version, headerLen, realSize, hashFns, bucketSize);
  }

  public byte getVersion() {
    return version;
  }

  public int getHeaderLength() {
    return headerLength;
  }

  public int getTotalLength() {
    return totalLength;
  }

  public int getHashFns() {
    return hashFns;
  }

  public BucketSize getBucketSize() {
    return bucketSize;
  }
}
