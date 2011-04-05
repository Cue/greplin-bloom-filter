/*
 * Copyright 2010 The Lucene Interval Field Authors.
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

/**
 * A probabalistic set data structure that supports deletions.
 * Guarantees it will never have a false negative
 * <p/>
 * N.B. Used to use memory mapped files, but they were really buggy in production
 * so switching to our on (probably slower - but safer), caching
 * Supports deletions
 */
public class BloomFilter implements Closeable {

  private static final int COUNT_BITS = 4; //DO NOT CHANGE. LOTS OF ASSUMPTIONS DEPEND ON THIS BEING 4
  private static final byte MAX_COUNT = (1 << COUNT_BITS) - 1;
  private static final int INT_SIZE = 32;
  private static final int META_DATA_OFFSET = 2 * INT_SIZE;
  private static final int BITS_IN_BYTE = 8;

  private RandomAccessFile file = null;
  private byte[] cache = null;

  private boolean cacheDirty;
  private boolean open;
  private final int hashFns;
  private final int realSize;   // the actual size of the bloom filter on disk (metadata, counting bits, etc)
  private final int pseudoSize; // we're equivalent to a non-counting bloom filter with this many positions
  private final RepeatedMurmurHash hash;


    /**
   * Opens an existing bloom filter.
   *
   * @param f the file to open
   * @return the bloom filter
   * @throws IOException if an I/O error is encountered
   */
  public static synchronized BloomFilter openExisting(File f) throws IOException {
    return new BloomFilter(f);
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
  public static synchronized BloomFilter createOptimal(File f, int numberOfItems,
                                                       double falsePositiveRate, boolean force)
      throws IOException {
    int bits = (int) Math.ceil((numberOfItems * Math.log(falsePositiveRate))
        / Math.log(1.0 / (Math.pow(2.0, Math.log(2.0)))));
    int hashFns = (int) Math.round(Math.log(2.0) * bits / numberOfItems);
    return new BloomFilter(f, bits, hashFns, force);
  }

  /**
   * Clears all elements from a bloom filter.
   */
  public synchronized void clear() {
    checkIfOpen();
    cache = new byte[bytes(realSize - META_DATA_OFFSET)];
    cacheDirty = true;
  }


  /**
   * Adds the given key to the bloom filter.
   *
   * @param data the key
   */
  public synchronized void add(byte[] data) {
    checkIfOpen();
    cacheDirty = true;
    int[] toSet = hash.hash(data);
    for (int i : toSet) {
      incrementCount(i);
    }
  }


  /**
   * Removes the given key from the bloom filter.
   *
   * @param data the key
   */
  public synchronized void remove(byte[] data) {
    checkIfOpen();
    cacheDirty = true;
    int[] toUnset = hash.hash(data);
    for (int i : toUnset) {
      decrementCount(i);
    }
  }


  /**
   * Checks if the bloom filter contains the key.
   *
   * @param data the key
   * @return whether the key likely is in the bloom filter.  if false, the key is definitely not in the bloom filter.
   *         if true, the key is probably in the bloom filter
   */
  public synchronized boolean contains(byte[] data) {
    checkIfOpen();
    int[] hash = this.hash.hash(data);
    for (int i : hash) {
      if (!this.isSet(i)) {
        return false;
      }
    }
    return true;
  }


  /**
   * Persists the bloom filter to disk.
   *
   * @throws IOException if I/O errors are encountered.
   */
  public synchronized void flush() throws IOException {
    checkIfOpen();
    if (file != null && cacheDirty) {
      file.seek(bytes(META_DATA_OFFSET));
      file.write(cache); // can probably be made more efficient
    }
  }


  /**
   * Closes the bloom filter.
   *
   * @throws IOException if I/O errors are encountered
   */
  public synchronized void close() throws IOException {
    if (open) {
      flush();
      if (file != null) {
        file.close();
      }
      cache = null;
      file = null;
      open = false;
    }
  }

  public int capacity(double falsePositiveRate) {
    int capacity = (int) Math.round((pseudoSize * Math.log(1.0 / Math.pow(2.0, Math.log(2.0))))
        / Math.log(falsePositiveRate));
    return capacity;
  }

  
  /**
   * Computes the number of bytes needed to fit the given number of bits.
   *
   * @param bits the number of bits
   * @return the number of bytes required
   */
  private static int bytes(int bits) {
    return bits / BITS_IN_BYTE + (bits % BITS_IN_BYTE == 0 ? 0 : 1);
  }


  /**
   * Creates a new bloom filter in the given file.
   *
   * @param f       the file to write to. If null, the bloom filter is just created in RAM
   * @param bits    the number of bits to use
   * @param hashFns the number of hash functions
   * @param force   whether to allow overwriting an existing file
   * @throws IOException if an I/O error occurs
   */
  private BloomFilter(File f, int bits, int hashFns, boolean force) throws IOException {
    realSize = bits * COUNT_BITS + META_DATA_OFFSET;
    pseudoSize = bits;
    this.hashFns = hashFns;
    hash = new RepeatedMurmurHash(hashFns, pseudoSize);

    // creating a new filter - so I can just be lazy and start it zero'd
    cache = new byte[bytes(realSize - META_DATA_OFFSET)];
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

      file = new RandomAccessFile(f, "rws");
      file.writeInt(hashFns);
      file.writeInt(realSize);
      file.setLength(bytes(realSize));

      flush();

      if (f != null && f.length() != bytes(realSize)) {
        throw new RuntimeException("Bad size - expected " + bytes(realSize) + " but got " + f.length());
      }
    }

  }

  private void checkIfOpen() {
    if (!open) {
      throw new IllegalStateException("Can't perform any operations on a closed bloom filter");
    }
  }

  /**
   * Opens an existing bloom filter.  Access via BloomFilter.openExisting(...)
   *
   * @param f the file to open
   * @throws IOException if I/O errors are encountered
   */
  private BloomFilter(File f) throws IOException {
    assert f.exists() && f.isFile() && f.canRead() && f.canWrite() : "Trying to open a non-existent bloom filter";

    file = new RandomAccessFile(f, "rws");

    hashFns = file.readInt();
    realSize = file.readInt();
    pseudoSize = (realSize - META_DATA_OFFSET) / COUNT_BITS;

    if (f.length() != bytes(realSize)) {
      throw new IllegalStateException(
          "Corrupted Bloom Filter: file size is " + f.length() + " but claims " + bytes(realSize));
    }

    // load the cache with the on disk data
    cache = new byte[bytes(realSize - META_DATA_OFFSET)];
    int readRes = file.read(cache);
    assert readRes == bytes(realSize - META_DATA_OFFSET)
        : "I only read " + readRes + " bytes, but was expecting " + bytes(realSize - META_DATA_OFFSET);

    hash = new RepeatedMurmurHash(hashFns, pseudoSize);
    open = true;
  }


  /**
   * Increments a count at the given hash table position
   *
   * @param position the position within pseudoSize
   */
  private void incrementCount(int position) {
    assert position >= 0 && position < pseudoSize;
    // I can only get/set bytes, but I want nibbles (4 bits). imagine we're looking at a 4 position filter:
    // 0000 0001 0010 0011
    // and let's say I'm interested in  position '1' (= 0001)
    // I have to get the full byte at position '0' (= 0000 0001),
    // and then store back a full byte (= 0000 0010) leaving me with the full result:
    // 0000 0010 0010 0011

    int bytePosition = position / 2;
    assert bytePosition < realSize;
    byte twoNibbles = cache[bytePosition];

    if ((position & 1) == 0) {
      // even position - so we want the high nibble
      byte myNibble = (byte) ((twoNibbles & 0xff) >>> 4);
      if (myNibble == MAX_COUNT) {
        return;
      }
      // can't just add 0x10 to twoNibblees because java can't do unsigned arithmetic
      myNibble += 1;
      twoNibbles = (byte) (twoNibbles & 0x0F);
      twoNibbles = (byte) (twoNibbles | (myNibble << 4));
      cache[bytePosition] = twoNibbles;
    } else { // odd position - so we want the low nibble
      byte myNibble = (byte) (twoNibbles & 0x0F);
      if (myNibble == MAX_COUNT) {
        return;
      }
      cache[bytePosition] = (byte) (twoNibbles + 1);
    }
  }


  /**
   * Decrements a count at the given hash table position
   *
   * @param position the position within pseudoSize
   */
  private void decrementCount(int position) {
    assert position >= 0 && position < pseudoSize;

    int bytePosition = position / 2;
    assert bytePosition < realSize;
    byte twoNibbles = cache[bytePosition];

    if ((position & 1) == 0) {
      // even position - so we want the high nibble
      byte myNibble = (byte) ((twoNibbles & 0xff) >>> 4);
      if (myNibble == MAX_COUNT || myNibble == 0) { // can't decr a MAX_COUNT
        return;
      }
      myNibble -= 1;
      twoNibbles = (byte) (twoNibbles & 0x0F);
      twoNibbles = (byte) (twoNibbles | (myNibble << 4));
      cache[bytePosition] = twoNibbles;
    } else { // odd position - so we want the low nibble
      byte myNibble = (byte) (twoNibbles & 0x0F);
      if (myNibble == MAX_COUNT || myNibble == 0) {
        return;
      }
      cache[bytePosition] = (byte) (twoNibbles - 1);
    }
  }


  /**
   * Checks if the given position is set at least once.
   *
   * @param position the position within pseudosize
   * @return whether it is set
   */
  private boolean isSet(int position) {
    assert position >= 0 && position < pseudoSize;
    if ((position & 1) == 0) {
      return cache[position / 2] >>> 4 != 0; // don't need to correct for sign/upconverting (it's still != 0)
    } else {
      // low nibble
      return (cache[position / 2] & 0x0F) != 0;
    }
  }

  // just for testing
  protected byte[] getUnderlyingDataBytes() {
    checkIfOpen();
    return cache;
  }
}
