greplin-bloom-filter
======================

Greplin Bloom Filter
---------------------

An (optionally persistent) counting Bloom Filter implementation in Java.

### Status:

This is a very early stage project.  It works for our needs.  We haven't verified it works beyond that.  Issue reports
and patches are very much appreciated!

For example, some obvious improvements include

* Support for a variable number of count bits (including 1)

* More efficient thread safety

* More hash functions to choose between

* More efficient disk persistence

### Pre-requisites:

[Maven] (http://maven.apache.org/)

## Installation

    git clone https://github.com/Greplin/greplin-bloom-filter.git

    cd greplin-bloom-filter

    mvn install

## Implementation details
   
* This is a counting bloom filter that uses 4-bits per bucket. Once the count exceeds 15 items in a bucket, decrements are no longer possible.

* Instead of using N distinct hashes, we use two linear combinations of two runs of a repeated murmur hash per [Kirch and Mitzenmacher] (http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf).


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