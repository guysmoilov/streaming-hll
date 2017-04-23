# streaming-hll
Extension to Clearspring impl. of HLL++, which allows merging directly from a stream.
Their implementation can be found here: https://github.com/addthis/stream-lib
I would like to thank the original creators, their implementation is amazing, elegant, and written as simply as it can be.
The only extension needed here was to better handle bulk merging of HLL++ instances, as efficiently as possible.

## Example usage
    StreamingHyperLogLogPlus target = new StreamingHyperLogLogPlus(14);
    HyperLogLogPlus source = new HyperLogLogPlus(14, 16);
    // offer some items to source....
    target.add(new ByteArrayInputStream( soure.getBytes() ));
    assert target.cardinality() == source.cardinality();

## Why the old dependency version?
The dependency on clearspring is set to 2.5.2 because that's the version Cassandra is using, at least in version 2.2.6
For my own practical reasons this is critical to my work.
However, there should be no reason this wouldn't work with newer versions of HLL++, 
though at time of writing I haven't tested this yet. 
