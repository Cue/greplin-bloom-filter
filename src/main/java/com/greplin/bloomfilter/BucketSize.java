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
