/*
 * Copyright 2010 Greplin, Inc. All Rights Reserved.
 */

package com.greplin.bloomfilter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to represent how many bits each bucket in the bloom filter should be. Once a bucket is full,
 * it can no longer be incremented or decremented (so any item whose hash includes that bucket can't be deleted).
 */
public enum BucketSize {
  ONE(1),
  TWO(2),
  FOUR(4),
  EIGHT(8);

  private final int bits;

  BucketSize(int bits) {
    this.bits = bits;
  }

  public int getBits() {
    return bits;
  }

  private static final Map<Integer, BucketSize> REVERSE_MAPPING;

  static {
    Map<Integer, BucketSize> builder = new HashMap<Integer, BucketSize>(BucketSize.values().length);
    for (BucketSize size : BucketSize.values()) {
      builder.put(size.getBits(), size);
    }
    REVERSE_MAPPING = Collections.unmodifiableMap(builder);
  }

  public static BucketSize getBucketSize(int bitsPerBucket) {
    return REVERSE_MAPPING.get(bitsPerBucket);
  }
}
