package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.util.Varint;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

/**
 * <p>
 * Created by guy.smoilov on 23/04/2017.
 * </p>
 */
public class StreamingHyperLogLogPlus extends HyperLogLogPlus {

    public static final int NORMAL_FORMAT_TYPE_INDICATOR = 0;
    public static final int SPARSE_FORMAT_TYPE_INDICATOR = 1;

    private static Field registerSetInternalMField;
    static {
        // This is very ugly, but unfortunately necessary unless we want to override everything in the superclass...
        registerSetInternalMField = null;
        try {
            registerSetInternalMField = RegisterSet.class.getDeclaredField("M");
            registerSetInternalMField.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private final int p;
    private int[] registerSetInternalM;

    public StreamingHyperLogLogPlus(int p) {
        super(p);
        this.p = p;

        RegisterSet ourRegisterSet = getRegisterSet();
        try {
            // Again, extremely ugly, but nothing for us to do with reasonable amount of work...
            registerSetInternalM = (int[]) registerSetInternalMField.get(ourRegisterSet);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
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
            readFromRegisterSet(dataInputStream);
        }
        else if (formatType == SPARSE_FORMAT_TYPE_INDICATOR) {
            readFromSparseSet(dataInputStream, otherP, otherSp);
        }
        else {
            throw new StreamingHyperLogLogPlusMergeException("Unknown format type " + formatType);
        }
    }

    private void readFromRegisterSet(DataInputStream dataInputStream) throws IOException {
        // We won't actually use this, but we have to read it or ruin the decoding
        int size = Varint.readUnsignedVarInt(dataInputStream);

        // The following is copied from RegisterSet.merge, with a few tweaks to avoid random access to the input stream
        for (int bucket = 0; bucket < registerSetInternalM.length; ++bucket) {
            int word = 0;
            int nexIntFromSource = dataInputStream.readInt();
            int nextIntFromUs = registerSetInternalM[bucket];
            for (int j = 0; j < RegisterSet.LOG2_BITS_PER_WORD; ++j) {
                int mask = 0x1f << (RegisterSet.REGISTER_SIZE * j);
                int thisVal = (nextIntFromUs & mask);
                int thatVal = (nexIntFromSource & mask);
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }

            registerSetInternalM[bucket] = word;
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
