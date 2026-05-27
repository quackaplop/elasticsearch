/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Random;

/**
 * Correctness coverage for the {@link PositionalNdJsonDecoder} fast path: exact-match against the
 * expected typed values, JSON escape handling, out-of-order fields, recognized {@code null}, and
 * the fallback signal ({@code false}) on shapes outside fast-path scope.
 */
public class PositionalNdJsonDecoderTests extends ESTestCase {

    private final BlockFactory factory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);

    private static final String[] NAMES = { "id", "count", "name", "ratio", "flag" };
    private static final DataType[] TYPES = { DataType.LONG, DataType.INTEGER, DataType.KEYWORD, DataType.DOUBLE, DataType.BOOLEAN };

    public void testScalarRowMatchesExpected() {
        decodeOne("{\"id\":42,\"count\":7,\"name\":\"hello\",\"ratio\":3.5,\"flag\":true}", row -> {
            assertEquals(42L, row.longAt(0));
            assertEquals(7, row.intAt(1));
            assertEquals(new BytesRef("hello"), row.bytesAt(2));
            assertEquals(3.5, row.doubleAt(3), 0.0);
            assertTrue(row.boolAt(4));
        });
    }

    public void testOutOfOrderFields() {
        decodeOne("{\"flag\":false,\"name\":\"x\",\"id\":-9,\"ratio\":0.0,\"count\":100}", row -> {
            assertEquals(-9L, row.longAt(0));
            assertEquals(100, row.intAt(1));
            assertEquals(new BytesRef("x"), row.bytesAt(2));
            assertFalse(row.boolAt(4));
        });
    }

    public void testEscapesMatchJacksonSemantics() {
        // Every simple escape plus a unicode-escaped BMP char and a surrogate-pair astral char.
        // "u" is split off so the literal backslash-u never appears adjacent in source (Java's
        // lexer runs the unicode-escape pass over the raw source, even inside string literals).
        String bsu = "\\" + "u"; // a literal backslash followed by 'u'
        String json = "{\"id\":1,\"count\":1,\"name\":\"a\\\"b\\\\c\\n\\t"
            + bsu
            + "00e9"
            + bsu
            + "D83D"
            + bsu
            + "DE00\",\"ratio\":1.0,\"flag\":true}";
        String expected = "a\"b\\c\n\té😀"; // what Jackson's getValueAsString() would yield
        decodeOne(json, row -> assertEquals(new BytesRef(expected.getBytes(StandardCharsets.UTF_8)), row.bytesAt(2)));
    }

    public void testExplicitNullAppendedInlineAndTouched() {
        BitSet touched = new BitSet();
        Block.Builder[] builders = newBuilders();
        Block[] blocks = null;
        try {
            byte[] b = bytes("{\"id\":5,\"count\":null,\"name\":\"n\",\"ratio\":2.0,\"flag\":false}");
            boolean ok = decoder().tryDecodeLine(b, 0, b.length, builders, touched);
            assertTrue(ok);
            assertTrue(touched.get(0));
            // Explicit JSON null is appended inline and the column is marked touched (matches
            // NdJsonPageDecoder); only ABSENT fields are left for the caller's end-of-line appendNull.
            assertTrue("explicit null must be appended inline and marked touched", touched.get(1));
            blocks = new Block[builders.length];
            for (int i = 0; i < builders.length; i++) {
                blocks[i] = builders[i].build();
            }
            assertTrue("count column position 0 must be null", blocks[1].isNull(0));
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
            Releasables.close(builders);
        }
    }

    public void testFallbackOnArray() {
        assertFalse(tryDecode("{\"id\":1,\"count\":1,\"name\":\"x\",\"ratio\":1.0,\"flag\":[1,2]}"));
    }

    public void testFallbackOnNestedObject() {
        assertFalse(tryDecode("{\"id\":1,\"count\":1,\"name\":{\"k\":1},\"ratio\":1.0,\"flag\":true}"));
    }

    public void testFallbackOnTypeMismatch() {
        // string where a long is expected -> coercion, punt to Jackson
        assertFalse(tryDecode("{\"id\":\"oops\",\"count\":1,\"name\":\"x\",\"ratio\":1.0,\"flag\":true}"));
    }

    public void testUnknownFieldSkipped() {
        decodeOne("{\"id\":1,\"extra\":\"ignored\",\"count\":2,\"name\":\"y\",\"ratio\":1.0,\"flag\":true}", row -> {
            assertEquals(1L, row.longAt(0));
            assertEquals(2, row.intAt(1));
            assertEquals(new BytesRef("y"), row.bytesAt(2));
        });
    }

    // ---- adaptive positional-plan behavior: best / re-learn / worst case ----

    public void testInOrderStaysOnFastPathAndNeverRelearns() {
        PositionalNdJsonDecoder dec = decoder();
        int[] inOrder = { 0, 1, 2, 3, 4 };
        for (int r = 0; r < 200; r++) {
            feedAndVerify(dec, inOrder, r);
        }
        assertTrue("in-order input must keep the fast path", dec.isFastPathEnabled());
        assertArrayEquals("schema order must be unchanged", new int[] { 0, 1, 2, 3, 4 }, dec.expectedOrder());
    }

    public void testConsistentlyPermutedFileRelearnsTheOrder() {
        PositionalNdJsonDecoder dec = decoder();
        int[] perm = { 2, 4, 0, 3, 1 }; // same reorder on every line
        for (int r = 0; r < 200; r++) {
            feedAndVerify(dec, perm, r);
        }
        assertTrue("a learnable order must keep the fast path", dec.isFastPathEnabled());
        assertArrayEquals("decoder must re-learn the file's field order", perm, dec.expectedOrder());
    }

    public void testPerLineShuffledGivesUpFastPathButStaysCorrect() {
        PositionalNdJsonDecoder dec = decoder();
        Random rnd = random();
        for (int r = 0; r < 200; r++) {
            int[] ord = { 0, 1, 2, 3, 4 };
            for (int i = ord.length - 1; i > 0; i--) { // Fisher-Yates: a fresh order every line
                int j = rnd.nextInt(i + 1);
                int t = ord[i];
                ord[i] = ord[j];
                ord[j] = t;
            }
            feedAndVerify(dec, ord, r); // correctness asserted every line regardless of order
        }
        assertFalse("an unlearnable (per-line) order must make the decoder give up the fast path", dec.isFastPathEnabled());
    }

    /** Emit one line with the 5 fields in {@code fieldOrder} (values keyed off {@code r}), decode, and verify every column. */
    private void feedAndVerify(PositionalNdJsonDecoder dec, int[] fieldOrder, int r) {
        long id = ((long) r << 20) + 7;
        int count = r * 3 + 1;
        String name = "n" + r;
        double ratio = r + 0.5;
        boolean flag = (r & 1) == 0;
        String[] frag = { "\"id\":" + id, "\"count\":" + count, "\"name\":\"" + name + "\"", "\"ratio\":" + ratio, "\"flag\":" + flag };
        StringBuilder sb = new StringBuilder("{");
        for (int j = 0; j < fieldOrder.length; j++) {
            if (j > 0) {
                sb.append(',');
            }
            sb.append(frag[fieldOrder[j]]);
        }
        sb.append('}');

        byte[] b = bytes(sb.toString());
        Block.Builder[] builders = newBuilders();
        Block[] blocks = null;
        try {
            assertTrue("row " + r + " must decode on the positional path", dec.tryDecodeLine(b, 0, b.length, builders, new BitSet()));
            blocks = new Block[builders.length];
            for (int i = 0; i < builders.length; i++) {
                blocks[i] = builders[i].build();
            }
            Row row = new Row(blocks);
            assertEquals(id, row.longAt(0));
            assertEquals(count, row.intAt(1));
            assertEquals(new BytesRef(name), row.bytesAt(2));
            assertEquals(ratio, row.doubleAt(3), 0.0);
            assertEquals(flag, row.boolAt(4));
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
            Releasables.close(builders);
        }
    }

    // ---- harness ----

    private PositionalNdJsonDecoder decoder() {
        return new PositionalNdJsonDecoder(NAMES, TYPES);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private boolean tryDecode(String json) {
        Block.Builder[] builders = newBuilders();
        try {
            byte[] b = bytes(json);
            return decoder().tryDecodeLine(b, 0, b.length, builders, new BitSet());
        } finally {
            Releasables.close(builders);
        }
    }

    private Block.Builder[] newBuilders() {
        return new Block.Builder[] {
            factory.newLongBlockBuilder(1),
            factory.newIntBlockBuilder(1),
            factory.newBytesRefBlockBuilder(1),
            factory.newDoubleBlockBuilder(1),
            factory.newBooleanBlockBuilder(1) };
    }

    private interface RowAsserts {
        void check(Row row);
    }

    private void decodeOne(String json, RowAsserts asserts) {
        Block.Builder[] builders = newBuilders();
        Block[] blocks = null;
        try {
            byte[] b = bytes(json);
            boolean ok = decoder().tryDecodeLine(b, 0, b.length, builders, new BitSet());
            assertTrue("expected fast-path success for: " + json, ok);
            blocks = new Block[builders.length];
            for (int i = 0; i < builders.length; i++) {
                blocks[i] = builders[i].build();
            }
            asserts.check(new Row(blocks));
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
            Releasables.close(builders);
        }
    }

    private record Row(Block[] blocks) {
        long longAt(int c) {
            return ((LongBlock) blocks[c]).getLong(0);
        }

        int intAt(int c) {
            return ((IntBlock) blocks[c]).getInt(0);
        }

        double doubleAt(int c) {
            return ((DoubleBlock) blocks[c]).getDouble(0);
        }

        boolean boolAt(int c) {
            return ((BooleanBlock) blocks[c]).getBoolean(0);
        }

        BytesRef bytesAt(int c) {
            return ((BytesRefBlock) blocks[c]).getBytesRef(0, new BytesRef());
        }
    }
}
