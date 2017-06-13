package com.clearspring.analytics.stream.cardinality;

import org.assertj.core.api.LongAssert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <p>
 * Created by guy.smoilov on 23/04/2017.
 * </p>
 */
public class StreamingHyperLogLogPlusTest {

    public static final int PRECISION = 10;
    public static final int SPARSE_CARDINALITY = 100;
    public static final int FULL_CARDINALITY = 10000;

    private final Random r = new Random();
    private HyperLogLogPlus src;
    private StreamingHyperLogLogPlus target;

    @Before
    public void setup() {
        src = new HyperLogLogPlus(PRECISION, PRECISION);
        target = new StreamingHyperLogLogPlus(PRECISION);
    }

    @Test
    public void sparseIntoEmpty() throws Exception {
        addSparse(src);
        addToTarget(src);

        long actualCardniality = target.cardinality();
        assertThat(actualCardniality).isEqualTo(src.cardinality());
        assertWithinMarginOfError(actualCardniality, SPARSE_CARDINALITY);
    }

    private void addSparse(ICardinality target) {
        addApproxItems(target, SPARSE_CARDINALITY);
    }

    private void addFull(ICardinality target) {
        addApproxItems(target, FULL_CARDINALITY);
    }

    private void addApproxItems(ICardinality target, int count) {
        for (int i = 0; i < count; i++) {
             target.offerHashed(r.nextLong());
        }
    }

    @Test
    public void sparseIntoSparse() throws Exception {
        addSparse(src);
        addSparse(target);

        long cardinalityBefore = target.cardinality();

        addToTarget(src);

        long actualCardniality = target.cardinality();
        assertThat(cardinalityBefore).isLessThan(actualCardniality);
        assertWithinMarginOfError(actualCardniality, 2 * SPARSE_CARDINALITY);
    }

    private LongAssert assertWithinMarginOfError(long actualCardniality, long expectedCardinality) {
        return assertThat(actualCardniality).isBetween((long) (expectedCardinality * 0.9), (long) (expectedCardinality * 1.1));
    }

    @Test
    public void sparseIntoRegisterSet() throws Exception {
        addSparse(src);
        addFull(target);

        long cardinalityBefore = target.cardinality();

        addToTarget(src);

        long actualCardniality = target.cardinality();
        assertThat(cardinalityBefore).isLessThan(actualCardniality);
        assertWithinMarginOfError(actualCardniality, FULL_CARDINALITY + SPARSE_CARDINALITY);
    }

    @Test
    public void registerSetIntoEmpty() throws Exception {
        addFull(src);
        addToTarget(src);

        long actualCardniality = target.cardinality();
        assertThat(actualCardniality).isEqualTo(src.cardinality());
        assertWithinMarginOfError(actualCardniality, FULL_CARDINALITY);
    }

    @Test
    public void registerSetIntoSparse() throws Exception {
        addFull(src);
        addSparse(target);

        long cardinalityBefore = target.cardinality();

        addToTarget(src);

        long actualCardniality = target.cardinality();
        assertThat(cardinalityBefore).isLessThan(actualCardniality);
        assertWithinMarginOfError(actualCardniality, FULL_CARDINALITY + SPARSE_CARDINALITY);
    }

    @Test
    public void registerSetIntoRegisterSet() throws Exception {
        addFull(src);
        addFull(target);

        long cardinalityBefore = target.cardinality();

        addToTarget(src);

        long actualCardniality = target.cardinality();
        assertThat(cardinalityBefore).isLessThan(actualCardniality);
        assertWithinMarginOfError(actualCardniality, 2 * FULL_CARDINALITY);
    }

    @Test
    public void serder_default() throws Exception {
        StreamingHyperLogLogPlus src = new StreamingHyperLogLogPlus(12);
        addFull(src);
        long cardinality = src.cardinality();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        src.writeToStream(outputStream);
        outputStream.close();

        StreamingHyperLogLogPlus target = new StreamingHyperLogLogPlus(12);
        target.addAll(new ByteArrayInputStream(outputStream.toByteArray()));

        assertThat(target.cardinality()).isEqualTo(cardinality);
    }

    private void serderTest(boolean writeUnsignedOnly, boolean readUnsignedOnly) throws IOException, CardinalityMergeException {
        StreamingHyperLogLogPlus src = new StreamingHyperLogLogPlus(12);
        addFull(src);
        long cardinality = src.cardinality();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        src.writeToStream(outputStream, writeUnsignedOnly);
        outputStream.close();

        StreamingHyperLogLogPlus target = new StreamingHyperLogLogPlus(12);
        target.addAll(new ByteArrayInputStream(outputStream.toByteArray()), readUnsignedOnly);

        assertThat(target.cardinality()).isEqualTo(cardinality);
    }

    @Test
    public void serder_unsigned_toUnsigned() throws Exception {
        serderTest(true, true);
    }

    @Test(expected = StreamingHyperLogLogPlus.StreamingHyperLogLogPlusMergeException.class)
    public void serder_unsigned_toSigned() throws Exception {
        serderTest(true, false);
    }

    @Test(expected = StreamingHyperLogLogPlus.StreamingHyperLogLogPlusMergeException.class)
    public void serder_signed_toUnsigned() throws Exception {
        serderTest(false, true);
    }

    @Test
    public void serder_signed_toSigned() throws Exception {
        serderTest(false, false);
    }

    @Test(expected = CardinalityMergeException.class)
    public void wrongPrecision_lower() throws Exception {
        HyperLogLogPlus src = new HyperLogLogPlus(PRECISION - 1);
        addToTarget(src);
    }

    @Test(expected = CardinalityMergeException.class)
    public void wrongPrecision_higher() throws Exception {
        HyperLogLogPlus src = new HyperLogLogPlus(PRECISION + 1);
        addToTarget(src);
    }

    private void addToTarget(HyperLogLogPlus src) throws CardinalityMergeException, IOException {
        target.addAll(new ByteArrayInputStream(src.getBytes()));
    }
}