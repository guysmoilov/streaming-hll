package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.Varint;

import java.io.*;
import java.util.*;

/**
 * Sadly, I was forced to copy a lot of the business logic of {@link HyperLogLogPlus} into this class, for the SOLE PURPOSE
 * of replacing the {@link RegisterSet} with a {@link StreamingRegisterSet}. Everything else was possible using inheritance...
 *
 * <p>
 * Created by guy.smoilov on 23/04/2017.
 * </p>
 */
public class StreamingHyperLogLogPlus implements ICardinality {

    /**
     * used to mark codec version for serialization
     */
    private static final int VERSION = 2;

    public static final int NORMAL_FORMAT_TYPE_INDICATOR = 0;
    public static final int SPARSE_FORMAT_TYPE_INDICATOR = 1;

    // The following fields copied from HyperLogLogPlus
    private final StreamingRegisterSet registerSet;
    private final int m;
    private final int p;

    private final double alphaMM;

    // TODO: Support having a sparse set as well. Right now, assuming we always have a register set simplifies things and is much more likely.
    /**
     * This constructor disables the sparse set.  If the counter is likely to exceed
     * the sparse set thresholds than using this constructor will help avoid the
     * extra memory pressure created by maintaining the sparse set until that threshold is
     * breached.
     *
     * @param p - the precision value for the normal set
     */
    public StreamingHyperLogLogPlus(int p)
    {
        this(p, new StreamingRegisterSet((int) Math.pow(2, p)));
    }

    protected StreamingHyperLogLogPlus(int p, StreamingRegisterSet registerSet)
    {
        if (p < 4)
        {
            throw new IllegalArgumentException("p must be more than 4 (inclusive)");
        }

        this.p = p;
        m = (int) Math.pow(2, p);
        this.registerSet = registerSet;

        // See the paper.
        switch (p)
        {
            case 4:
                alphaMM = 0.673 * m * m;
                break;
            case 5:
                alphaMM = 0.697 * m * m;
                break;
            case 6:
                alphaMM = 0.709 * m * m;
                break;
            default:
                alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }

    /**
     * Like {@link #addAll(StreamingHyperLogLogPlus)}, but doesn't deserialize the source bytes into a new instance of {@link HyperLogLogPlus}.
     * Extracting the data from the input stream is done by replicating the logic from {@link HyperLogLogPlus.Builder#build(byte[])},
     * and adding the data one piece at a time.
     * This allows us to scalably merge many serialized instances of {@link HyperLogLogPlus} into one.
     *
     * @param inputStream Should contain the output of {@link HyperLogLogPlus#getBytes()}.
     */
    public void addAll(InputStream inputStream) throws CardinalityMergeException, IOException {
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
        getRegisterSet().merge(dataInputStream);
    }

    protected void readFromSparseSet(DataInputStream dataInputStream, int otherP, int otherSp) throws IOException {
        int size = Varint.readUnsignedVarInt(dataInputStream);
        StreamingRegisterSet ourRegisterSet = getRegisterSet();
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

    @Override
    public boolean offerHashed(long hashedLong)
    {
        // find first p bits of x
        final long idx = hashedLong >>> (64 - p);
        //Ignore the first p bits (the idx), and then find the number of leading zeros
        //Push a 1 to where the bit string would have ended if we didnt just push the idx out of the way
        //A one is always added to runLength for estimation calculation purposes
        final int runLength = Long.numberOfLeadingZeros((hashedLong << this.p) | (1 << (this.p - 1))) + 1;
        return registerSet.updateIfGreater((int)idx, runLength);
    }

    @Override
    public boolean offerHashed(int hashedInt)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Add data to estimator based on the mode it is in
     *
     * @param o stream element
     * @return Will almost always return true for sparse mode because the additions are batched in
     */
    @Override
    public boolean offer(Object o)
    {
        long x = MurmurHash.hash64(o);
        return offerHashed(x);
    }

    /**
     * Gather the cardinality estimate from this estimator.
     * <p/>
     * Has two procedures based on current mode. 'Normal' mode works similar to HLL but has some
     * new bias corrections. 'Sparse' mode is linear counting.
     *
     * @return
     */
    @Override
    public long cardinality()
    {
        double registerSum = 0;
        int count = registerSet.count;
        double zeros = 0;
        for (int j = 0; j < registerSet.count; j++)
        {
            int val = registerSet.get(j);
            registerSum += 1.0 / (1<<val);
            if (val == 0) {
                zeros++;
            }
        }

        double estimate = alphaMM * (1 / registerSum);
        double estimatePrime = estimate;
        if (estimate <= (5 * m))
        {
            estimatePrime = estimate - getEstimateBias(estimate, p);
        }
        double H;
        if (zeros > 0)
        {
            H = count * Math.log(count / zeros);
        }
        else
        {
            H = estimatePrime;
        }
        // when p is large the threshold is just 5*m
        if ((p <= 18 && H < HyperLogLogPlus.thresholdData[p - 4]) || (p > 18 && estimate <= 5 * m))
        {
            return Math.round(H);
        }
        else
        {
            return Math.round(estimatePrime);
        }
    }

    private double getEstimateBias(double estimate, int p)
    {
        // get nearest neighbors for this estimate and precision
        // above p = 18 there is no bias correction
        if (p > 18)
        {
            return 0;
        }
        double[] estimateVector = HyperLogLogPlus.rawEstimateData[p - 4];
        SortedMap<Double, Integer> estimateDistances = calcDistances(estimate, estimateVector);
        int[] nearestNeighbors = getNearestNeighbors(estimateDistances);
        return getBias(nearestNeighbors);
    }

    private double getBias(int[] nearestNeighbors)
    {

        double[] biasVector = HyperLogLogPlus.biasData[p - 4];
        double biasTotal = 0.0d;
        for (int nearestNeighbor : nearestNeighbors)
        {
            biasTotal += biasVector[nearestNeighbor];
        }
        return biasTotal / (nearestNeighbors.length);
    }

    private int[] getNearestNeighbors(SortedMap<Double, Integer> distanceMap)
    {
        int[] nearest = new int[6];
        int i = 0;
        for (Integer index : distanceMap.values())
        {
            nearest[i++] = index;
            if (i >= 6)
            {
                break;
            }
        }
        return nearest;
    }

    private SortedMap<Double, Integer> calcDistances(double estimate, double[] estimateVector)
    {
        SortedMap<Double, Integer> distances = new TreeMap<Double, Integer>();
        int index = 0;
        for (double anEstimateVector : estimateVector)
        {
            distances.put(Math.pow(estimate - anEstimateVector, 2), index++);
        }
        return distances;
    }

    @Override
    public int sizeof()
    {
        return registerSet.size * 4;
    }

    @Override
    public byte[] getBytes() throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        // write version flag (always negative)
        writeToStream(dos);
        return baos.toByteArray();
    }

    /**
     * Similar to {@link #getBytes()}, but writes the bytes directly to the stream.
     */
    public void writeToStream(OutputStream outputStream) throws IOException {
        writeToStream(new DataOutputStream(outputStream));
    }

    private void writeToStream(DataOutputStream dos) throws IOException {
        dos.writeInt(-VERSION);
        Varint.writeUnsignedVarInt(p, dos);
        Varint.writeUnsignedVarInt(0, dos); // For sp
        Varint.writeUnsignedVarInt(0, dos);
        Varint.writeUnsignedVarInt(registerSet.size * 4, dos);
        for (int x : registerSet.bits())
        {
            dos.writeInt(x);
        }
    }

    /**
     * Add all the elements of the other set to this set.
     *
     * If possible, the sparse mode is protected. A switch to the normal mode
     * is triggered only if the resulting set exceed the threshold.
     *
     * This operation does not imply a loss of precision.
     *
     * @param other A compatible Hyperloglog++ instance (same p and sp)
     * @throws CardinalityMergeException if other is not compatible
     */
    public void addAll(StreamingHyperLogLogPlus other) throws HyperLogLogPlus.HyperLogLogPlusMergeException
    {
        if (other.sizeof() != sizeof())
        {
            throw new HyperLogLogPlus.HyperLogLogPlusMergeException("Cannot merge estimators of different sizes");
        }

        registerSet.merge(other.registerSet);
    }

    /**
     * Merge this HLL++ with a bunch of others! The power of minions!
     * <p/>
     * Most of the logic consists of case analysis about the state of this HLL++ and each one it wants to merge
     * with. If either of them is 'normal' mode then the other converts to 'normal' as well. A touching sacrifice.
     * 'Normal's combine just like regular HLL estimators do.
     * <p/>
     * If they happen to be both sparse, then it checks if their combined size would be too large and if so, they get
     * relegated to normal mode anyway. Otherwise, the mergeEstimators function is called, and a new sparse HLL++ is born.
     *
     * @param estimators the estimators to merge with this one
     * @return a new estimator with their combined knowledge
     * @throws CardinalityMergeException
     */

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException
    {
        StreamingHyperLogLogPlus merged = new StreamingHyperLogLogPlus(p);
        merged.addAll(this);

        if (estimators == null)
        {
            return merged;
        }

        for (ICardinality estimator : estimators)
        {
            if (!(estimator instanceof StreamingHyperLogLogPlus))
            {
                throw new HyperLogLogPlus.HyperLogLogPlusMergeException("Cannot merge estimators of different class");
            }
            StreamingHyperLogLogPlus hll = (StreamingHyperLogLogPlus) estimator;
            merged.addAll(hll);
        }

        return merged;
    }

    /** exposed for testing */
    protected StreamingRegisterSet getRegisterSet() {
        return registerSet;
    }
}
