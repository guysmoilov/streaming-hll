package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.util.Varint;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * <p>
 * Created by guy.smoilov on 23/04/2017.
 * </p>
 */
public class StreamingHyperLogLogPlus extends HyperLogLogPlus {

    public static final int NORMAL_FORMAT_TYPE_INDICATOR = 0;
    public static final int SPARSE_FORMAT_TYPE_INDICATOR = 1;

    private final int p;

    public StreamingHyperLogLogPlus(int p) {
        super(p);
        this.p = p;
    }

    // TODO: Support having a sparse set as well. Right now, assuming we always have a register set simplifies things and is much more likely.

    /**
     * Like {@link #addAll(HyperLogLogPlus)}, but doesn't deserialize the source bytes into a new instance of {@link HyperLogLogPlus}.
     * Extracting the data from the input stream is done by replicating the logic from {@link HyperLogLogPlus.Builder#build(byte[])},
     * and adding the data one piece at a time.
     * This allows us to scalably merge many serialized instances of {@link HyperLogLogPlus} into one.
     *
     * @param inputStream Should contain the output of {@link HyperLogLogPlus#getBytes()}.
     */
    public void add(InputStream inputStream) throws CardinalityMergeException, IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        int version = dataInputStream.readInt();
        if (version < 0) {
            decodeBytes(dataInputStream);
        }
        else {
            throw new StreamingHyperLogLogPlusMergeException("Legacy decode not supported yet");
        }

    }

    protected void decodeBytes(DataInputStream dataInputStream) throws IOException,
                                                                     StreamingHyperLogLogPlusMergeException {
        int otherP = Varint.readUnsignedVarInt(dataInputStream);

        if (this.p != otherP) {
            throw new StreamingHyperLogLogPlusMergeException(String.format("Other's precision is %d instead of %d", otherP, this.p));
        }

        int otherSp = Varint.readUnsignedVarInt(dataInputStream);
        int formatType = Varint.readUnsignedVarInt(dataInputStream);
        if (formatType == NORMAL_FORMAT_TYPE_INDICATOR) {
            int size = Varint.readUnsignedVarInt(dataInputStream);
            // TODO: There is a byte array of |size| in the input stream, we need to replicate Bits.getBits logic combined with
            // the addAll logic from HLLP
        }
        else if (formatType == SPARSE_FORMAT_TYPE_INDICATOR) {
            readFromSparseSet(dataInputStream, otherP, otherSp);
        }
        else {
            throw new StreamingHyperLogLogPlusMergeException("Unknown format type " + formatType);
        }
    }

    protected void readFromSparseSet(DataInputStream dataInputStream, int otherP, int otherSp) throws IOException {
        int size = Varint.readUnsignedVarInt(dataInputStream);
        RegisterSet ourRegisterSet = getRegisterSet();
        int prevDeltaRead = 0;
        for (int i = 0; i < size; ++i) {
            // The following calculation is copied from the build(byte[]) method, they save deltas instead of full ints to save space.
            int nextVal = Varint.readUnsignedVarInt(dataInputStream) + prevDeltaRead;

            // The following lines (and additional required methods) appear in addAll, in the case where we are
            // NORMAL and other is SPARSE. I left the opaque naming as-is.
            int idx = getIndex(nextVal, this.p, otherSp);
            int r = decodeRunLength(nextVal, otherP, otherSp);
            ourRegisterSet.updateIfGreater(idx, r);

            prevDeltaRead = nextVal;
        }
    }

    /**
     * Copied from {@link HyperLogLogPlus#getSparseIndex(int)}
     */
    private static int getSparseIndex(int k)
    {
        if ((k & 1) == 1) {
            return k >>> 7;
        }
        else {
            return k >>> 1;
        }
    }

    /**
     * Copied from {@link HyperLogLogPlus#getIndex(int, int)}
     */
    private static int getIndex(int k, int p, int sp)
    {
        k = getSparseIndex(k);
        return (k >>> (sp - p));
    }

    /**
     * Copied from {@link HyperLogLogPlus#decodeRunLength(int)}
     */

    private static int decodeRunLength(int k, int p, int sp)
    {
        if ((k & 1) == 1) {
            return ((k >>> 1) & 63) ^ 63;
        }
        else {
            return Integer.numberOfLeadingZeros(k << p + (31 - sp)) + 1;
        }
    }

    public class StreamingHyperLogLogPlusMergeException extends CardinalityMergeException {

        public StreamingHyperLogLogPlusMergeException(String message) {
            super(message);
        }
    }
}
