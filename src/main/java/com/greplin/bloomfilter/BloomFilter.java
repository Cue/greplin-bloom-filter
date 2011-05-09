/*
 * Copyright 2010 The Greplin Bloom Filter Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.greplin.bloomfilter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A probabalistic set data structure that supports deletions.
 * Guarantees it will never have a false negative
 * <p/>
 * N.B. Used to use memory mapped files, but they were really buggy in production
 * so switching to our on (probably slower - but safer), caching
 * Supports deletions
 */
public class BloomFilter implements Closeable {

  private static final int BITS_IN_BYTE = 8;
  private static final int DEFAULT_SEEK_THRESHOLD = 20; // See the TestFileIO class for details on how this was decided
  private static final BucketSize DEFAULT_BUCKET_SIZE = BucketSize.FOUR;

  private final RandomAccessFile file;
  private byte[] cache = null;

  // Note: unflushedChanges is always implemented as a ConcurrentSkipListMap for a few reasons
  // First, iteration is ordered based on its keys, and its more efficient to seek to each change in order
  // Second, because it's always small (elements<SEEK_THRESHOLD) the O(log(N)) operations aren't a big deal,
  // and are almost always outweighed by benefits of being lock-free.
  // One caveat is that the size() method is O(n) time, so we keep an independent size counter.
  private final Map<Integer, Byte> unflushedChanges;
  private final AtomicInteger unflushedChangeCounter = new AtomicInteger(0);

  private volatile boolean cacheDirty;
  private volatile boolean open;

  private final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();

  // how many bytes have to change before we just rewrite the entire file. At some point, a big sequential write
  // becomes cheaper than a bunch of seeks
  private final int seekThreshold;

  private final RepeatedMurmurHash hash;
  private BloomMetadata metadata;


  /**
   * Opens an existing bloom filter.
   *
   * @param f the file to open
   * @return the bloom filter
   * @throws IOException if an I/O error is encountered
   */
  public static BloomFilter openExisting(File f) throws IOException {
    return openExisting(f, DEFAULT_SEEK_THRESHOLD);
  }

  public static BloomFilter openExisting(File f, int seekThreshold) throws IOException {
    return new BloomFilter(f, seekThreshold);
  }

  /**
   * Create an optimal bloom filter for the given expected number of items and desired false positive rate.
   * Set force true to delete the file if it already exists.
   *
   * @param f                 the file to write to
   * @param numberOfItems     the number of expected items in the bloom filter
   * @param falsePositiveRate allowable false positive rate when at capacity
   * @param force             whether to allow overwriting the file. If force is false and the file already exists,
   *                          will throw an IllegalArgumentException
   * @return a bloom filter backed by the file
   * @throws IOException if I/O errors are encountered
   */
  public static BloomFilter createOptimal(File f, int numberOfItems,
                                          double falsePositiveRate, boolean force)
      throws IOException {
    return createOptimal(f, numberOfItems, falsePositiveRate, force, DEFAULT_SEEK_THRESHOLD, DEFAULT_BUCKET_SIZE);
  }

  public static BloomFilter createOptimal(File f, int numberOfItems,
                                          double falsePositiveRate, boolean force,
                                          int seekThreshold,
                                          BucketSize countBits)
      throws IOException {
    int buckets = (int) Math.ceil((numberOfItems * Math.log(falsePositiveRate))
        / Math.log(1.0 / (Math.pow(2.0, Math.log(2.0)))));
    int hashFns = (int) Math.round(Math.log(2.0) * buckets / numberOfItems);
    return new BloomFilter(f, buckets, hashFns, force, seekThreshold, countBits);
  }

  /**
   * Clears all elements from a bloom filter.
   */
  public void clear() {
    cacheLock.writeLock().lock();
    try {
      checkIfOpen();
      cache = new byte[this.metadata.getTotalLength() - this.metadata.getHeaderLength()];
      cacheDirty = true;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }


  /**
   * Adds the given key to the bloom filter.
   *
   * @param data the key
   */
  public void add(byte[] data) {
    int[] toSet = hash.hash(data);
    cacheLock.writeLock().lock();
    try {
      checkIfOpen();
      for (int i : toSet) {
        incrementCount(i);
      }
      cacheDirty = true;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }


  /**
   * Removes the given key from the bloom filter.
   *
   * @param data the key
   */
  public void remove(byte[] data) {
    int[] toUnset = hash.hash(data);
    cacheLock.writeLock().lock();
    try {
      checkIfOpen();
      for (int i : toUnset) {
        decrementCount(i);
      }
      cacheDirty = true;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }


  /**
   * Checks if the bloom filter contains the key.
   *
   * @param data the key
   * @return whether the key likely is in the bloom filter.  if false, the key is definitely not in the bloom filter.
   *         if true, the key is probably in the bloom filter
   */
  public boolean contains(byte[] data) {
    int[] hash = this.hash.hash(data);
    cacheLock.readLock().lock();
    try {
      checkIfOpen();
      for (int i : hash) {
        if (!this.isSet(i)) {
          return false;
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return true;
  }


  /**
   * Persists the bloom filter to disk.
   *
   * @throws IOException if I/O errors are encountered.
   */
  public void flush() throws IOException {
    cacheLock.writeLock().lock();
    try {
      checkIfOpen();
      if (cacheDirty && unflushedChanges != null && file != null) {
        final int offset = this.metadata.getHeaderLength();

        //it's actually a disk-backed filter with changes
        if (unflushedChangeCounter.get() >= seekThreshold) {
          file.seek(offset);
          file.write(cache); // can probably be made more efficient
          file.getFD().sync();
        } else {
          for (Map.Entry<Integer, Byte> change : unflushedChanges.entrySet()) {
            file.seek(change.getKey() + offset);
            file.write(change.getValue());
          }
        }
        cacheDirty = false;
        unflushedChanges.clear();
        unflushedChangeCounter.set(0);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }


  /**
   * Closes the bloom filter.
   *
   * @throws IOException if I/O errors are encountered
   */
  public void close() throws IOException {
    cacheLock.writeLock().lock();
    try {
      if (open) {
        flush();
        if (file != null) {
          file.close();
        }
        cache = null;
        open = false;
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public int capacity(double falsePositiveRate) {
    int capacity = (int) Math.round((this.metadata.getBucketCount() * Math.log(1.0 / Math.pow(2.0, Math.log(2.0))))
        / Math.log(falsePositiveRate));
    return capacity;
  }


  private void checkIfOpen() {
    if (!open) {
      throw new IllegalStateException("Can't perform any operations on a closed bloom filter");
    }
  }

  /**
   * Creates a new bloom filter in the given file.
   *
   * @param f             the file to write to. If null, the bloom filter is just created in RAM
   * @param buckets          the number of bits to use
   * @param hashFns       the number of hash functions
   * @param force         whether to allow overwriting an existing file
   * @param seekThreshold How many changes we should take before just rewriting the whole file on flush
   * @throws IOException if an I/O error occurs
   */
  private BloomFilter(File f, int buckets, int hashFns, boolean force, int seekThreshold, BucketSize countBits)
      throws IOException {
    this.seekThreshold = seekThreshold;
    this.metadata = BloomMetadata.createNew(buckets, hashFns, countBits);
    hash = new RepeatedMurmurHash(hashFns, this.metadata.getBucketCount());

    // creating a new filter - so I can just be lazy and start it zero'd
    cache = new byte[this.metadata.getTotalLength() - this.metadata.getHeaderLength()];
    cacheDirty = true;

    open = true;

    if (f != null) {
      if (f.exists()) {
        if (force) {
          if (!f.delete()) {
            throw new IOException("Couldn't delete old file at " + f.getAbsolutePath());
          }
        } else {
          throw new IllegalArgumentException("Can't create a new BloomFilter at " + f.getAbsolutePath()
              + " since it already exists");
        }
      }

      file = new RandomAccessFile(f, "rw");
      this.metadata.writeToFile(file);
      file.setLength(metadata.getTotalLength());
      file.getFD().sync();
      unflushedChanges = new ConcurrentSkipListMap<Integer, Byte>();

      if (f.length() != metadata.getTotalLength()) {
        throw new RuntimeException("Bad size - expected " + metadata.getTotalLength() + " but got " + f.length());
      }
    } else {
      unflushedChanges = null; // don't bother keeping track of unflushed changes if this is memory only
      file = null;
    }

  }

  /**
   * Opens an existing bloom filter.  Access via BloomFilter.openExisting(...)
   *
   * @param f             the file to open
   * @param seekThreshold How many changes we should take before just rewriting the whole file on flush
   * @throws IOException if I/O errors are encountered
   */
  private BloomFilter(File f, int seekThreshold) throws IOException {
    assert f.exists() && f.isFile() && f.canRead() && f.canWrite() : "Trying to open a non-existent bloom filter";
    this.seekThreshold = seekThreshold;

    file = new RandomAccessFile(f, "rw");
    this.metadata = BloomMetadata.readHeader(file);
    unflushedChanges = new ConcurrentSkipListMap<Integer, Byte>();

    // load the cache with the on disk data
    cache = new byte[metadata.getTotalLength() - metadata.getHeaderLength()];
    int readRes = file.read(cache);
    assert readRes == (metadata.getTotalLength() - metadata.getHeaderLength())
        : "I only read " + readRes + " bytes, but was expecting " + (metadata.getTotalLength()
        - metadata.getHeaderLength());

    hash = new RepeatedMurmurHash(metadata.getHashFns(), metadata.getBucketCount());
    open = true;
  }


  private void setByte(int position, byte value) {
    assert cacheLock.isWriteLockedByCurrentThread();
    cache[position] = value;
    if (unflushedChanges != null && unflushedChangeCounter.get() < seekThreshold) {
      unflushedChanges.put(position, value);
      unflushedChangeCounter.incrementAndGet();
    }
  }


  // return the integer value of a (possibly improper) subset of bits from a given byte.
  // e.g., if byte x = 01101101, then getNumAt(x, 0, 2) = b01 = d1
  // or getNumAt(x, 4, 4) = b1101 = d13
  protected static byte getBucketAt(byte data, final int offset, final int len) {
    assert offset < BITS_IN_BYTE;
    assert len <= BITS_IN_BYTE;
    assert offset + len <= BITS_IN_BYTE;

    // shift so the bits we want are right-most
    final int shift = (BITS_IN_BYTE - (offset + len));
    data = (byte) (data >>> shift);

    // zero out everything to the left of what we're interested in
    // Java needs the 0xFF & 0xFF since it doesn't have unsigned types, and silently converts bytes to ints. Don't ask.
    final byte mask = (byte) ((0xFF & 0xFF) >> (BITS_IN_BYTE - len));
    data = (byte) (data & mask);

    return data;
  }

  protected static byte putBucketAt(final byte wholeByte, int offset, int len, byte bucketVal) {
    assert offset < BITS_IN_BYTE;
    assert len <= BITS_IN_BYTE;
    assert offset + len <= BITS_IN_BYTE;
    assert bucketVal <= ((1 << len) - 1);

    byte res = wholeByte;

    // first we want to clear the old value of the bucket
    byte mask = (byte) ((1 << len) - 1);
    mask <<= (BITS_IN_BYTE - (offset + len));
    mask = (byte) ~mask; // this is a little annoying, but it works for buckets that are in the 'middle' of a byte
    res &= mask;

    // then we want to set the bits in the bucket correctly
    bucketVal <<= (BITS_IN_BYTE - (offset + len));
    res |= bucketVal;

    return res;
  }


  /**
   * Checks if the given bucket has a count of at least one.
   *
   * @param bucket - the bucket you're interested in
   * @return whether it is set
   */
  private boolean isSet(int bucket) {
    assert bucket >= 0 && bucket < this.metadata.getBucketCount();
    final int indexOfByteContainingBucket = bucket / this.metadata.getBucketsPerByte();
    assert indexOfByteContainingBucket < this.metadata.getTotalLength();

    final int offsetOfBucketInByte = (bucket % this.metadata.getBucketsPerByte())
        * this.metadata.getBucketSize().getBits();
    final byte byteContainingBucket = cache[indexOfByteContainingBucket];
    final byte bucketVal = getBucketAt(byteContainingBucket, offsetOfBucketInByte,
        this.metadata.getBucketSize().getBits());

    return bucketVal != 0;
  }

  // if decr is false, then it's an incr
  private void modifyBucket(int position, boolean decr) {
    assert position >= 0 && position < this.metadata.getBucketCount();

    final int indexOfByteContainingBucket = position / this.metadata.getBucketsPerByte();
    assert indexOfByteContainingBucket < this.metadata.getTotalLength();
    final byte byteContainingBucket = cache[indexOfByteContainingBucket];

    final int offsetOfBucketInByte = (position % this.metadata.getBucketsPerByte()) * this.metadata.getBucketSize().getBits();
    final int bucket = getBucketAt(byteContainingBucket, offsetOfBucketInByte, this.metadata.getBucketSize().getBits());

    // bucket is overflowing, can't do anything
    if (bucket == this.metadata.getMaxCountInBucket()) {
      return;
    }

    assert bucket < this.metadata.getMaxCountInBucket();

    int newBucketVal;
    if (decr) {
      newBucketVal = bucket - 1;
    } else {
      newBucketVal = bucket + 1;
    }

    byte newVal =
        putBucketAt(byteContainingBucket, offsetOfBucketInByte, this.metadata.getBucketSize().getBits(), (byte) newBucketVal);
    setByte(indexOfByteContainingBucket, newVal);
  }


  /**
   * Increments the count in a given bucket
   *
   * @param bucket - which bucket to increment
   */
  private void incrementCount(int bucket) {
    modifyBucket(bucket, false);
  }


  /**
   * Decrements the count in a given bucket
   *
   * @param bucket - which bucket to increment
   */  private void decrementCount(int bucket) {
    modifyBucket(bucket, true);
  }

  // just for testing
  protected byte[] getUnderlyingDataBytes() {
    checkIfOpen();
    return cache;
  }
}
