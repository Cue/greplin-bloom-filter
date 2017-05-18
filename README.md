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

* The NewBuilder and OpenBuilder expose a lot of (optionally) tunable knobs and hooks. Most users will never need to know about them though, and just supplying the required Builder constructor arguments will be fine.

* This is a counting bloom filter that uses a configurable number of bits per bucket. If you use one bit per bucket, then items can never be deleted. If you use 8 bits per bucket, then it uses 8x more space than a non-counting filter, but items can be deleted as long as the count doesn't exceed 255 items in a bucket.

* Instead of using N distinct hashes, we use linear combinations of two runs of a repeated murmur hash per [Kirch and Mitzenmacher] (https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf).

* If you are using a persistent bloom filter, then on flush we intelligently decide to either rewrite the entire file or or just seek into the file and change particular bytes based on how much of the file has changed. 

* Has relatively efficient thread-safety via a ReentrantReadWriteLock

* No external dependencies (besides JUnit - which is only needed to run the test suite)

## Usage

    // if the 'file' is null, the bloom filter is in-memory only, and not-persisted to disk
    final File onDiskFile = new File("/tmp/greplin-bloom-filter.bin");
    final int expectedItems = 10000;
    final double desiredFalsePositiveRate = 0.000001;

    final byte[] exampleItemA = "Hello World!".getBytes(Charset.forName("UTF-8"));
    final byte[] exampleItemB = "Goodbye Cruel world".getBytes(Charset.forName("UTF-8"));

    BloomFilter bloomFilter = new BloomFilter.NewBuilder(onDiskFile, expectedItems, desiredFalsePositiveRate)
        .force(true) // tells it to over-write any existing file at onDiskFile
        .build();

    System.out.println(bloomFilter.contains(exampleItemA)); // false
    System.out.println(bloomFilter.contains(exampleItemB)); // false

    bloomFilter.add(exampleItemA);
    bloomFilter.add(exampleItemB);

    System.out.println(bloomFilter.contains(exampleItemA)); // true
    System.out.println(bloomFilter.contains(exampleItemB)); // true

    bloomFilter.remove(exampleItemB);

    System.out.println(bloomFilter.contains(exampleItemA)); // true
    System.out.println(bloomFilter.contains(exampleItemB)); // false

    bloomFilter.close();
    bloomFilter = null;


    // now, let's reopen the same bloom filter from the on-disk file
    bloomFilter = new BloomFilter.OpenBuilder(onDiskFile).build();

    System.out.println(bloomFilter.contains(exampleItemA)); // true
    System.out.println(bloomFilter.contains(exampleItemB)); // false

    bloomFilter.remove(exampleItemA);

    System.out.println(bloomFilter.contains(exampleItemA)); // false
    System.out.println(bloomFilter.contains(exampleItemB)); // false

    bloomFilter.close();

## Authors
[Greplin, Inc.](http://www.greplin.com)