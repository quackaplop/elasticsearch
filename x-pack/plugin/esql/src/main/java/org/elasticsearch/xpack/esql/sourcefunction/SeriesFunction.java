/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationBuilder;

import java.math.BigInteger;

/**
 * A run of whole numbers, as one column named after the function.
 * <p>
 * Two of them, differing only in whether {@code stop} is reached, because the two engines a user is most likely to
 * be coming from disagree and both spellings are common: {@code generate_series(start, stop[, step])} includes
 * {@code stop}, as it does in ClickHouse and DuckDB, and {@code range(start, stop[, step])} excludes it, as it does
 * in DuckDB. Naming them apart is the whole of the difference, which is better than one name whose end condition a
 * reader has to remember.
 * <p>
 * The single-argument spellings those engines also accept — {@code range(5)} for {@code 0..4} — are deliberately
 * not here. They change what the first position <em>means</em> depending on how many arguments follow, and a
 * signature states one meaning per position. Someone who wants that writes the start.
 * <p>
 * The rows are built on the coordinator and held for the life of the query, charged to nothing, so the count is
 * bounded. Every argument is a constant, so the count is arithmetic rather than a guess: {@link #validate} works it
 * out and refuses an oversized call before a single block is allocated.
 */
public final class SeriesFunction extends CoordinatorRowsFunction {

    /**
     * The most rows one call may produce. A constant rather than a setting because nothing hands a source function
     * node settings today; {@code EsqlPlugin.sourceFunctions} is the seam that would, by widening to take what it
     * needs. The number matches the default of {@code esql.query.result_truncation_max_size}, on the reasoning that
     * a source nobody can read the whole of is not worth materializing.
     */
    static final long MAX_ROWS = 10_000;

    private final String name;
    private final boolean inclusive;

    private SeriesFunction(String name, boolean inclusive) {
        this.name = name;
        this.inclusive = inclusive;
    }

    /** {@code generate_series(start, stop[, step])}, which reaches {@code stop}. */
    public static SeriesFunction generateSeries() {
        return new SeriesFunction("generate_series", true);
    }

    /** {@code range(start, stop[, step])}, which stops short of {@code stop}. */
    public static SeriesFunction range() {
        return new SeriesFunction("range", false);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SourceFunctionSignature signature() {
        return SourceFunctionSignature.of(
            SourceFunctionSignature.wholeNumber("start"),
            SourceFunctionSignature.wholeNumber("stop"),
            SourceFunctionSignature.optional(SourceFunctionSignature.wholeNumber("step"))
        );
    }

    @Override
    public void validate(SourceFunctionInvocation call) {
        long step = step(call);
        if (step == 0) {
            // Separately from the count, because a zero step never terminates rather than producing too many rows,
            // and "takes forever" and "is too big" are different things to be told.
            throw new ParsingException(call.source(), "[{}] needs a step other than zero", name);
        }
        long rows = rowCount(call.number(0), call.number(1), step, inclusive);
        if (rows > MAX_ROWS) {
            throw new ParsingException(
                call.source(),
                "[{}] would produce [{}] rows, more than the [{}] it may hold on the coordinator",
                name,
                rows,
                MAX_ROWS
            );
        }
    }

    /**
     * The step the call wrote, or one. Always one, never minus one for a {@code stop} below {@code start}: both
     * reference implementations default to a positive step and hand back nothing for a descending pair, so a
     * descending run is written with a negative step. Inferring the direction would be friendlier and would make
     * the same query mean different things here and there, which is worse.
     */
    private static long step(SourceFunctionInvocation call) {
        Long written = call.number(2);
        return written != null ? written : 1;
    }

    /**
     * How many values the run holds, in {@link BigInteger} because {@code stop - start} overflows a long for
     * bounds a user can perfectly well write — {@code range(-9223372036854775808, 9223372036854775807)} — and an
     * overflowed span would wrap to a small count and be allowed through.
     */
    static long rowCount(long start, long stop, long step, boolean inclusive) {
        BigInteger span = BigInteger.valueOf(stop).subtract(BigInteger.valueOf(start));
        if (span.signum() == 0) {
            return inclusive ? 1 : 0;
        }
        BigInteger stepValue = BigInteger.valueOf(step);
        if (span.signum() != stepValue.signum()) {
            // The step walks away from stop, so the run is empty rather than endless. Empty rather than refused,
            // because a bound computed by whatever produced the query may legitimately come out this way.
            return 0;
        }
        // Both operands share a sign, so the quotient is non-negative and truncation is toward zero.
        BigInteger[] whole = span.divideAndRemainder(stepValue);
        BigInteger count = whole[0];
        if (inclusive || whole[1].signum() != 0) {
            count = count.add(BigInteger.ONE);
        }
        return count.bitLength() > 62 ? Long.MAX_VALUE : count.longValueExact();
    }

    @Override
    protected void write(
        SourceFunctionInvocation call,
        ProjectMetadata projectMetadata,
        LocalRelationBuilder rows,
        ActionListener<Void> listener
    ) {
        computed(listener, built -> {
            long start = call.number(0);
            long step = step(call);
            long count = rowCount(start, call.number(1), step, inclusive);
            LocalRelationBuilder.Longs column = built.longs(name);
            long value = start;
            for (long i = 0; i < count; i++) {
                column.append(value);
                value += step;
            }
        }, rows);
    }
}
