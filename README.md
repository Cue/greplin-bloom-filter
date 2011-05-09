greplin-bloom-filter
======================

Greplin Bloom Filter
---------------------

An Bloom Filter implementation in Java, that optionally supports persistence and counting buckets.

### Status:

This is a very early stage project.  It works for our needs.  We haven't verified it works beyond that.  Issue reports
and patches are very much appreciated!

Some improvements we'd love to see include:

* Optimized code paths for particular sized counting buckets

* More hash functions to choose between

* Variable cache sizes and types. Enabling, for example, a lower-memory read only mode or a smaller cache that performs disk seeks to perform some operations

* Support on-the fly bucket expansion, possibly via a [d-left counting bloom filter] (http://theory.stanford.edu/~rinap/papers/esa2006b.pdf)

### Pre-requisites:

[Maven] (http://maven.apache.org/)

## Installation

    git clone https://github.com/Greplin/greplin-bloom-filter.git

    cd greplin-bloom-filter

    mvn install

## Implementation details
   
* This is a counting bloom filter that uses a configurable number of bits per bucket. If you use one bit per bucket, then items can never be deleted. If you use 8 bits per bucket, then it uses 8x more space than a non-counting filter, but items can be deleted as long as the count doesn't exceed 255 items in a bucket.

* Instead of using N distinct hashes, we use linear combinations of two runs of a repeated murmur hash per [Kirch and Mitzenmacher] (http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf).

* If you are using a persistent bloom filter, then on flush we intelligently decide to either rewrite the entire file or or just seek into the file and change particular bytes based on how much of the file has changed. 

* Has relatively efficient thread-safety via a ReentrantReadWriteLock

* No external dependencies (besides JUnit - which is only needed to run the test suite)

## Usage
    // create a new bloom filter with the desired set of properties
    final int expectedNumberOfItems = 10000;
    final double desiredFalsePositiveRate = 0.00001;
    BloomFilter bf = BloomFilter.createOptimal("/tmp/bloom.dat", expectedNumberOfItems, desiredFalsePositiveRate, true);
   
    // test out the bloom filter
    bf.add("Hello World".getBytes());
    System.out.println(bf.contains("Hello World".getBytes()));
    System.out.println(bf.contains("Foo Bar".getBytes()));

    // persist it to disk (note that it is only persisted to disk when you call flush)
    bf.flush()

    // try removing an item
    bf.remove("Hello World".getBytes());
    System.out.println(bf.contains("Hello World".getBytes()));

    // close the bloom filter (which also persists any unflushed changes)
    bf.close();

## Authors
[Greplin, Inc.](http://www.greplin.com)