package com.clearspring.analytics.stream.cardinality;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Mostly copied from {@link StreamingRegisterSet}.
 * The only addition is {@link #merge(DataInputStream)}.
 *
 * <p>
 * Created by guy.smoilov on 29/05/2017.
 * </p>
 */
public class StreamingRegisterSet {

    public final static int LOG2_BITS_PER_WORD = 6;
    public final static int REGISTER_SIZE = 5;

    public final int count;
    public final int size;

    private final int[] M;

    public StreamingRegisterSet(int count)
    {
        this(count, null);
    }

    public StreamingRegisterSet(int count, int[] initialValues)
    {
        this.count = count;
        int bits = getBits(count);

        if (initialValues == null)
        {
            if (bits == 0)
            {
                this.M = new int[1];
            }
            else if (bits % Integer.SIZE == 0)
            {
                this.M = new int[bits];
            }
            else
            {
                this.M = new int[bits + 1];
            }
        }
        else
        {
            this.M = initialValues;
        }
        this.size = this.M.length;
    }

    public static int getBits(int count)
    {
        return count / LOG2_BITS_PER_WORD;
    }

    public void set(int position, int value)
    {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        this.M[bucketPos] = (this.M[bucketPos] & ~(0x1f << shift)) | (value << shift);
    }

    public int get(int position)
    {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.M[bucketPos] & (0x1f << shift)) >>> shift;
    }

    public boolean updateIfGreater(int position, int value)
    {
        int bucket = position / LOG2_BITS_PER_WORD;
        int shift  = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
        int mask = 0x1f << shift;

        // Use long to avoid sign issues with the left-most shift
        long curVal = this.M[bucket] & mask;
        long newVal = value << shift;
        if (curVal < newVal) {
            this.M[bucket] = (int)((this.M[bucket] & ~mask) | newVal);
            return true;
        } else {
            return false;
        }
    }

    public void merge(StreamingRegisterSet that)
    {
        for (int bucket = 0; bucket < M.length; bucket++)
        {
            int word = 0;
            for (int j = 0; j < LOG2_BITS_PER_WORD; j++)
            {
                int mask = 0x1f << (REGISTER_SIZE * j);

                int thisVal = (this.M[bucket] & mask);
                int thatVal = (that.M[bucket] & mask);
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }
            this.M[bucket] = word;
        }
    }

    public void merge(DataInputStream dataInputStream) throws IOException {
        // The following is copied from RegisterSet.merge, with a few tweaks to avoid random access to the input stream
        for (int bucket = 0; bucket < M.length; ++bucket) {
            int word = 0;
            int nexIntFromSource = dataInputStream.readInt();
            int nextIntFromUs = M[bucket];
            for (int j = 0; j < RegisterSet.LOG2_BITS_PER_WORD; ++j) {
                int mask = 0x1f << (RegisterSet.REGISTER_SIZE * j);
                int thisVal = (nextIntFromUs & mask);
                int thatVal = (nexIntFromSource & mask);
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }

            M[bucket] = word;
        }
    }

    public int[] bits()
    {
        int[] copy = new int[size];
        System.arraycopy(M, 0, copy, 0, M.length);
        return copy;
    }

    /**
     * If this was present in {@link RegisterSet}, all this copy-pasting would not be necessary
     */
    protected void setDirectly(int position, int value) {
        this.M[position] = value;
    }

    /**
     * If this was present in {@link RegisterSet}, all this copy-pasting would not be necessary
     */
    protected int getDirectly(int position) {
        return this.M[position];
    }
}
