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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Some basic benchmarks - nothing too formal.
 */
public class BloomBenchmark {

  public static final int ELEMENT_SIZE = 2500;
  public static final int ELEMENT_COUNT = 50000;
  public static final double FALSE_POSITIVE_RATE = 0.0001;

  public static final int BENCHMARK_RUNS = 100;

  private static final Random RANDOM = new Random();

  private static final byte[][] DATA = new byte[ELEMENT_COUNT][ELEMENT_SIZE];

  private static final List<Long> HASH_SIZES = new LinkedList<Long>();
  private static final List<Long> BLOOM_SIZES = new LinkedList<Long>();
  
  static {
    for (int i = 0; i < ELEMENT_COUNT; i++) {
      RANDOM.nextBytes(DATA[i]);
    }
  }

  private static class Benchmark {
    private final String name;
    private final Runnable r;

    private Benchmark(String name, Runnable r) {
      this.name = name;
      this.r = r;
    }

    public long run() {
      long startTime = System.currentTimeMillis();
      r.run();
      return System.currentTimeMillis() - startTime;
    }

    public String getName() {
      return name;
    }
  }

  public static Map<String, List<Long>> runBenchmarks(Collection<Benchmark> benchmarks) {
    Map<String, List<Long>> results = new HashMap<String, List<Long>>();
    for (Benchmark benchmark : benchmarks) {
      assert results.get(benchmark.getName()) == null
          : "Can't have two benchmarks with the same name (" + benchmark.getName() + ")";
      List<Long> times = new LinkedList<Long>();

      System.out.println("Running benchmark for " + benchmark.getName());
      for (int i = 0; i < BENCHMARK_RUNS; i++) {
        times.add(benchmark.run());
      }
      results.put(benchmark.getName(), times);
    }

    return results;
  }

  private static final Benchmark DYNAMIC_HASH_SET = new Benchmark("Dynamic Hash Set",
      new Runnable() {
        @Override
        public void run() {
          Set<byte[]> hashSet = new HashSet<byte[]>();
          for (byte[] element : DATA) {
            hashSet.add(element);
          }
        }
      });

  private static final Benchmark PRESIZED_HASH_SET = new Benchmark("Presized Hash Set",
      new Runnable() {
        @Override
        public void run() {
          Set<byte[]> hashSet = new HashSet<byte[]>(ELEMENT_COUNT);
          for (byte[] element : DATA) {
            hashSet.add(element);
          }
        }
      });

  private static final Benchmark PERSISTENT_HASH_SET = new Benchmark("Persistent Hash Set",
      new Runnable() {
        @Override
        public void run() {
          Set<byte[]> hashSet = new HashSet<byte[]>(ELEMENT_COUNT);
          for (byte[] element : DATA) {
            hashSet.add(element);
          }

          try {
            File f = File.createTempFile("hashSet", "test");
            FileOutputStream fos = new FileOutputStream(f);
            ObjectOutputStream out = new ObjectOutputStream(fos);
            out.writeObject(hashSet);
            out.close();
            HASH_SIZES.add(f.length());
          } catch (IOException e) {
            throw new RuntimeException("Error writing persistent hashset", e);
          }
        }
      });

  private static final Benchmark BLOOM_FILTER = new Benchmark("Bloom Filter",
      new Runnable() {
        @Override
        public void run() {
          try {
            BloomFilter bf = BloomFilter.createOptimal(null, ELEMENT_COUNT, FALSE_POSITIVE_RATE, true);
            for (byte[] element : DATA) {
              bf.add(element);
            }
            bf.close();
          } catch (IOException e) {
            throw new RuntimeException("Error with in-memory Bloom Filter", e);
          }
        }
      });

    private static final Benchmark PERSISTENT_BLOOM_FILTER = new Benchmark("Persistent Bloom Filter",
      new Runnable() {
        @Override
        public void run() {
          try {
            File f = File.createTempFile("bloomFilter", "test");
            BloomFilter bf = BloomFilter.createOptimal(f, ELEMENT_COUNT, FALSE_POSITIVE_RATE, true);
            for (byte[] element : DATA) {
              bf.add(element);
            }
            bf.close();
            BLOOM_SIZES.add(f.length());
          } catch (IOException e) {
            throw new RuntimeException("Error persisting Bloom Filter", e);
          }
        }
      });

  private static double computeMean(Collection<Long> data) {
    long total = 0;
    int counter = 0;

    for (Long datum : data) {
      total += datum;
      counter++;
    }

    return total/counter;
  }

  private static double computeStandardDeviation(Collection<Long> data, double mean) {
    double sumSquaredDeviations = 0;
    int counter = 0;
    for (Long datum : data) {
      counter++;
      sumSquaredDeviations += Math.pow((mean - datum), 2);
    }

    return Math.sqrt(sumSquaredDeviations/(counter - 1));
  }

  public static void main(String[] args) throws IOException {
    List<Benchmark> benchmarks = Arrays.asList(DYNAMIC_HASH_SET, PRESIZED_HASH_SET, PERSISTENT_HASH_SET,
        BLOOM_FILTER, PERSISTENT_BLOOM_FILTER);

    Map<String, List<Long>> benchmarkResults = runBenchmarks(benchmarks);

    System.out.print("\n\n");

    for (Map.Entry<String, List<Long>> benchmark : benchmarkResults.entrySet()) {
      double mean = computeMean(benchmark.getValue());
      double stdDev = computeStandardDeviation(benchmark.getValue(), mean);
      System.out.println(benchmark.getKey() + " took an average of " + mean
          + "ms with a standard deviation of " + stdDev + "ms");
    }

    System.out.println("\nAverage bloom filter size: " + computeMean(BLOOM_SIZES) + " bytes");
    System.out.println("Average hashset size: " + computeMean(HASH_SIZES) + " bytes");
  }
}
