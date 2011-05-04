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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Basic sanity checks on the bloom filter
 */
public class BloomFilterTest {
  private static final String[] IN = {"hello sweet world",
      "goodbye cruel world",
      "Bloomfilter",
      "what is with java and bit twiddling?"};

  private static final String[] OUT = {"and another one",
      "greplin got nominated for a crunchie!",
      "why? I dunno",
      "sdalkf sdkljfds"};

  private static final File TEMP_FILE;

  static {
    try {
      TEMP_FILE = File.createTempFile("greplin-bloom", "test");
    } catch (IOException e) {
      throw new RuntimeException("Unable to create new temp file", e);
    }
  }

  @Test
  public void testBasic() throws IOException {
    BloomFilter bf = BloomFilter.createOptimal(TEMP_FILE, 1000, 0.00001, true);

    for (String s : IN) {
      bf.add(s.getBytes());
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    for (String s : OUT) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }

    for (String s : IN) {
      Assert.assertTrue(bf.contains(s.getBytes()));
    }
  }

  @Test
  public void testOpenClose() throws IOException {
    BloomFilter bf = BloomFilter.createOptimal(TEMP_FILE, 1000, 0.00001, true);

    for (String s : IN) {
      bf.add(s.getBytes());
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    for (String s : OUT) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }

    for (String s : IN) {
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    int originalCapacity = bf.capacity(0.00001);
    Assert.assertEquals(1000, originalCapacity);
    byte[] exactData = bf.getUnderlyingDataBytes();
    bf.close();
    bf = null;

    bf = BloomFilter.openExisting(TEMP_FILE);
    for (String s : OUT) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }

    for (String s : IN) {
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    Assert.assertArrayEquals(exactData, bf.getUnderlyingDataBytes());
    Assert.assertEquals(originalCapacity, bf.capacity(0.00001));
    bf.close();
  }

  @Test
  public void testMetadataOverwrite() throws IOException {
    // Ensures that metadata cannot be overwritten by hash data.

    BloomFilter bf = BloomFilter.createOptimal(TEMP_FILE, 1000, 0.00001, true);

    bf.add("".getBytes());
    Assert.assertTrue(bf.contains("".getBytes()));

    bf.flush();
    bf.close();

    bf = BloomFilter.openExisting(TEMP_FILE);

    Assert.assertTrue(bf.contains("".getBytes()));
  }

  @Test
  public void testSerialize() throws IOException {
    BloomFilter bf = BloomFilter.createOptimal(TEMP_FILE, 1000, 0.00001, true);

    for (String s : IN) {
      bf.add(s.getBytes());
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    for (String s : OUT) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }

    bf.flush();
    bf.close();

    bf = BloomFilter.openExisting(TEMP_FILE);

    for (String s : IN) {
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    for (String s : OUT) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }
  }

  @Test
  public void testSeekThreshold() throws IOException {
    int[] thresholdsToTest = {0, 1, 2, 5, 10, 100, 1000};
    for (int i : thresholdsToTest) {
      BloomFilter bf = BloomFilter.createOptimal(TEMP_FILE, 1000, 0.00001, true, i);

      for (String s : IN) {
        bf.add(s.getBytes());
        Assert.assertTrue(bf.contains(s.getBytes()));
      }

      for (String s : OUT) {
        Assert.assertFalse(bf.contains(s.getBytes()));
      }

      bf.flush();
      bf.close();

      bf = BloomFilter.openExisting(TEMP_FILE);

      for (String s : IN) {
        Assert.assertTrue(bf.contains(s.getBytes()));
      }

      for (String s : OUT) {
        Assert.assertFalse(bf.contains(s.getBytes()));
      }
    }
  }

  @Test
  public void testCapacity() throws IOException {
    BloomFilter bf = BloomFilter.createOptimal(TEMP_FILE, 1000, 0.00001, true);
    Assert.assertEquals(1000, bf.capacity(0.00001));
  }

  @Test
  public void testRemove() throws IOException {
    BloomFilter bf = BloomFilter.createOptimal(TEMP_FILE, 1000, 0.00001, true);

    for (String s : IN) {
      bf.add(s.getBytes());
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    for (String s : OUT) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }


    for (String s : IN) {
      Assert.assertTrue(bf.contains(s.getBytes()));
    }

    for (int i = 0; i < IN.length; i++) {
      bf.remove(IN[i].getBytes());
      Assert.assertFalse(bf.contains(IN[i].getBytes()));

      for (int j = i + 1; j < IN.length; j++) {
        Assert.assertTrue(bf.contains(IN[j].getBytes()));
      }
    }

    for (String s : OUT) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }
    for (String s : IN) {
      Assert.assertFalse(bf.contains(s.getBytes()));
    }
  }

  @Test
  public void testFalsePositiveRate() throws IOException {
    BloomFilter bf = BloomFilter.createOptimal(null, 1000, 0.01, false);

    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] item = new byte[100];
      r.nextBytes(item);
      bf.add(item);
      Assert.assertTrue(bf.contains(item));
    }

    int falsePositives = 0;
    // theoretically, we could generate they same random 100 bytes
    // that were previous inserted. but that's ludicrously unlikely
    for (int i = 0; i < 1000; i++) {
      byte[] item = new byte[100];
      r.nextBytes(item);
      if (bf.contains(item)) {
        falsePositives += 1;
      }
    }

    // we expect 10 false positives. We should get more than 30 less than one in a million runs
    // see: http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
    Assert.assertTrue(falsePositives < 30);
  }
}
