/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * The arithmetic is the whole of these two functions, so most of this is the edges of it: where the run stops,
 * which way it walks, and what it does with bounds no loop should be asked to walk between.
 */
public class SeriesFunctionTests extends ESTestCase {

    public void testGenerateSeriesReachesStop() {
        assertThat(values(SeriesFunction.generateSeries(), 1L, 5L, null), equalTo(List.of(1L, 2L, 3L, 4L, 5L)));
    }

    public void testRangeStopsShortOfStop() {
        assertThat(values(SeriesFunction.range(), 1L, 5L, null), equalTo(List.of(1L, 2L, 3L, 4L)));
    }

    /** A step that does not divide the span: both stop at the last value before the bound, which is the same value. */
    public void testAStepThatDoesNotDivideTheSpan() {
        assertThat(values(SeriesFunction.generateSeries(), 0L, 5L, 2L), equalTo(List.of(0L, 2L, 4L)));
        assertThat(values(SeriesFunction.range(), 0L, 5L, 2L), equalTo(List.of(0L, 2L, 4L)));
    }

    /** And where it does divide it, which is the only case the two disagree about. */
    public void testAStepThatDividesTheSpan() {
        assertThat(values(SeriesFunction.generateSeries(), 0L, 6L, 2L), equalTo(List.of(0L, 2L, 4L, 6L)));
        assertThat(values(SeriesFunction.range(), 0L, 6L, 2L), equalTo(List.of(0L, 2L, 4L)));
    }

    public void testANegativeStepWalksDown() {
        assertThat(values(SeriesFunction.generateSeries(), 5L, 1L, -1L), equalTo(List.of(5L, 4L, 3L, 2L, 1L)));
        assertThat(values(SeriesFunction.range(), 5L, 1L, -1L), equalTo(List.of(5L, 4L, 3L, 2L)));
    }

    /**
     * A stop below start with the default step is empty rather than descending. Both reference implementations
     * default to a positive step, and inferring the direction here would make the same query mean one thing in
     * ES|QL and another in the engine the user copied it from.
     */
    public void testADescendingPairWithTheDefaultStepIsEmpty() {
        assertThat(values(SeriesFunction.generateSeries(), 5L, 1L, null), empty());
        assertThat(values(SeriesFunction.range(), 5L, 1L, null), empty());
    }

    /** Start equal to stop is the one row for the inclusive spelling and none for the other. */
    public void testStartEqualToStop() {
        assertThat(values(SeriesFunction.generateSeries(), 3L, 3L, null), equalTo(List.of(3L)));
        assertThat(values(SeriesFunction.range(), 3L, 3L, null), empty());
    }

    /** A zero step never terminates, which is a different thing to be told than that the run is too big. */
    public void testAZeroStepIsRefused() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> SeriesFunction.range().validate(call("range", 1L, 5L, 0L))
        );
        assertThat(e.getMessage(), containsString("[range] needs a step other than zero"));
    }

    public void testMoreRowsThanTheCeilingIsRefused() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> SeriesFunction.range().validate(call("range", 0L, SeriesFunction.MAX_ROWS + 1, null))
        );
        assertThat(e.getMessage(), containsString("would produce [10001] rows"));
        assertThat(e.getMessage(), containsString("more than the [10000]"));
    }

    /** And the row either side of the ceiling, so the comparison cannot be off by one without this failing. */
    public void testExactlyTheCeilingStands() {
        SeriesFunction.range().validate(call("range", 0L, SeriesFunction.MAX_ROWS, null));
    }

    /**
     * Bounds whose span overflows a long. Written out rather than randomized because the whole point is the pair
     * that wraps: computed in longs, {@code stop - start} here is -1, which would read as an empty run and let a
     * call asking for every long through to the loop.
     */
    public void testASpanThatOverflowsALongIsStillTooBig() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> SeriesFunction.range().validate(call("range", Long.MIN_VALUE, Long.MAX_VALUE, null))
        );
        assertThat(e.getMessage(), containsString("more than the [10000]"));
    }

    public void testTheColumnIsNamedAfterTheFunction() {
        assertThat(relation(SeriesFunction.generateSeries(), 1L, 2L, null).output().get(0).name(), equalTo("generate_series"));
        assertThat(relation(SeriesFunction.range(), 1L, 2L, null).output().get(0).name(), equalTo("range"));
    }

    /**
     * The facility asks once per place a call appears and installs what comes back untouched, so two asks must
     * not hand back one page: the first consumer to read it would release blocks the second still needs.
     */
    public void testEachAskBuildsItsOwnPage() {
        SeriesFunction function = SeriesFunction.range();
        LocalRelation first = relation(function, 1L, 4L, null);
        LocalRelation second = relation(function, 1L, 4L, null);

        assertNotSame(first, second);
        Page firstPage = first.supplier().get();
        Page secondPage = second.supplier().get();
        try {
            assertNotSame(firstPage, secondPage);
            assertNotSame(firstPage.getBlock(0), secondPage.getBlock(0));
            assertThat(read(firstPage), equalTo(read(secondPage)));
            assertThat(read(firstPage), not(empty()));
        } finally {
            firstPage.releaseBlocks();
            secondPage.releaseBlocks();
        }
    }

    private static List<Long> values(SeriesFunction function, long start, long stop, Long step) {
        Page page = relation(function, start, stop, step).supplier().get();
        try {
            return read(page);
        } finally {
            page.releaseBlocks();
        }
    }

    private static List<Long> read(Page page) {
        LongBlock block = page.getBlock(0);
        List<Long> values = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            values.add(block.getLong(i));
        }
        return values;
    }

    private static LocalRelation relation(SeriesFunction function, long start, long stop, Long step) {
        SourceFunctionInvocation call = call(function.name(), start, stop, step);
        function.signature().check(call);
        function.validate(call);
        PlainActionFuture<LeafPlan> future = new PlainActionFuture<>();
        function.toRelation(call, List.of(), null, future);
        return (LocalRelation) future.actionGet();
    }

    private static SourceFunctionInvocation call(String name, Long... arguments) {
        List<SourceFunctionArgument> list = Arrays.stream(arguments)
            .filter(Objects::nonNull)
            .<SourceFunctionArgument>map(value -> new SourceFunctionArgument.Value(Source.EMPTY, DataType.LONG, value))
            .toList();
        return new SourceFunctionInvocation(Source.EMPTY, name, list);
    }
}
