/*
 * Copyright 2010 Greplin, Inc. All Rights Reserved.
 */

package com.greplin.bloomfilter;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Represents the metadata associated with a serialized bloom filter.
 * <p/>
 * The old header format was:
 * 4 bytes for the number of hash Fns
 * 4 bytes for the total file size (data + metadata) in bits
 * That header format always assumed 4-bit buckets
 * <p/>
 * The header format is now as follows:
 * 8 bytes of zeroes (to distinguish this format from the old one)
 * 3 bytes for the 'magic word' which is: 0xB1 0xF1 0xCA
 * 1 byte for the header version (currently 2 - implicitly 1 in the old header format)
 * 4 bytes for header length in bytes (currently 32 bytes)
 * 4 bytes for real-size in bytes (total size of data + metadata)
 * 4 bytes for the number of hash fns
 * 4 bytes for the number of counting-bits in each bucket
 * 4 bytes of 0 padding to make the whole header 32-bytes even
 * The new format has the first two bytes as '0', which the old format will never have - so we can safely identify
 * which is which. If we detect the old format, we can safely assume there are four bits per bucket.
 * This is a little convoluted, but it's the safest way to guarantee backwards compatibility with the old format
 */
class BloomMetadata {

  private static final int INT_SIZE = 4;
  private static final int BITS_IN_BYTE = 8;
  private static final byte VERSION = 2;
  private static final byte[] MAGIC_WORD = {(byte) 0xB1, (byte) 0xF1, (byte) 0xCA};
  private static final int EXPECTED_HEADER_BYTES = 32;

  private final byte version;
  private final int headerLength;
  private final int totalLength;
  private final int hashFns;
  private final BucketSize bucketSize;

  private final int maxCountInBucket;
  private final int bucketsPerByte;
  private final int bucketCount;

  public static BloomMetadata readHeader(RandomAccessFile buffer) throws IOException {
    final int firstInt = buffer.readInt();
    final int secondInt = buffer.readInt();

    if (firstInt == 0 && secondInt == 0) {
      return readNewStyleHeader(buffer);
    } else {
      return readOldStyleHeader(buffer, firstInt, secondInt);
    }
  }

  public static BloomMetadata createNew(final int buckets, final int hashFns, final BucketSize countBits)
      throws IOException {
    return new BloomMetadata(null, VERSION, EXPECTED_HEADER_BYTES,
        EXPECTED_HEADER_BYTES + bytes(buckets * countBits.getBits()), hashFns, countBits);
  }

  private BloomMetadata(RandomAccessFile file,
                        byte version, int headerLength, int totalLength, int hashFns, BucketSize bucketSize)
      throws IOException {
    this.version = version;
    this.headerLength = headerLength;
    this.totalLength = totalLength;
    this.hashFns = hashFns;
    this.bucketSize = bucketSize;
    this.maxCountInBucket = (1 << this.bucketSize.getBits()) - 1;
    this.bucketsPerByte = BITS_IN_BYTE / this.bucketSize.getBits();
    this.bucketCount = (this.totalLength - this.headerLength) * this.bucketsPerByte;

    if (hashFns <= 0) {
      throw new InvalidBloomFilter("Invalid number of hashFns (" + hashFns + " bytes)");
    }

    if (this.totalLength < this.headerLength) {
      throw new InvalidBloomFilter("Impossibly short size (" + totalLength + " bytes)");
    }

    if (file != null && file.length() != totalLength) {
      throw new InvalidBloomFilter("Expected a file length of " + totalLength + " but only got " + file.length());
    }
  }

  private static int bytes(int bits) {
    return bits / BITS_IN_BYTE + (bits % BITS_IN_BYTE == 0 ? 0 : 1);
  }

  private static BloomMetadata readOldStyleHeader(RandomAccessFile file, int hashFns, int realSize)
      throws IOException {
    return new BloomMetadata(file, (byte) 1, 2 * INT_SIZE, realSize, hashFns, BucketSize.FOUR);
  }

  private static BloomMetadata readNewStyleHeader(RandomAccessFile buffer) throws IOException {

    // verify the magic word is present and intact
    final byte[] shouldBeMagicWord = new byte[MAGIC_WORD.length];
    buffer.read(shouldBeMagicWord);
    if (!Arrays.equals(MAGIC_WORD, shouldBeMagicWord)) {
      throw new InvalidBloomFilter("Invalid Magic Word " + Arrays.toString(shouldBeMagicWord));
    }

    // verify the version is correct
    final byte version = buffer.readByte();
    if (!(version == VERSION)) {
      throw new InvalidBloomFilter("Unrecognized version (" + version + ")");
    }

    final int headerLen = buffer.readInt();
    if (headerLen != EXPECTED_HEADER_BYTES) {
      throw new InvalidBloomFilter("Unexpected header length (" + headerLen + " bytes)");
    }

    final int realSize = buffer.readInt();

    final int hashFns = buffer.readInt();

    final int bucketSizeInt = buffer.readInt();
    final BucketSize bucketSize = BucketSize.getBucketSize(bucketSizeInt);
    if (bucketSize == null) {
      throw new InvalidBloomFilter("Invalid bucketSize (" + bucketSize + " bytes)");
    }

    if (buffer.readInt() != 0) {
      throw new InvalidBloomFilter("Invalid end padding");
    }

    return new BloomMetadata(buffer, version, headerLen, realSize, hashFns, bucketSize);
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

  public int getMaxCountInBucket() {
    return maxCountInBucket;
  }

  public int getBucketsPerByte() {
    return bucketsPerByte;
  }

  public int getBucketCount() {
    return bucketCount;
  }

  public void writeToFile(RandomAccessFile file) throws IOException {
    if (getVersion() == 1) {
      assert getHeaderLength() == 2 * INT_SIZE;
      file.writeInt(getHashFns());
      file.writeInt(getTotalLength());
    } else {
      assert getVersion() == VERSION;
      file.writeInt(0);                         // 4 bytes
      file.writeInt(0);                         // 8 bytes
      file.write(MAGIC_WORD);                   // 11 bytes
      file.writeByte(VERSION);                  // 12 bytes
      file.writeInt(EXPECTED_HEADER_BYTES);     // 16 bytes
      file.writeInt(getTotalLength());          // 20 bytes
      file.writeInt(getHashFns());              // 24 bytes
      file.writeInt(getBucketSize().getBits()); // 28 bytes
      file.writeInt(0);                         // 32 bytes
    }

  }
}
