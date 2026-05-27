/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Differential correctness: for a large randomized space of NDJSON records — random field order,
 * extra unknown fields, missing columns, explicit nulls, escaped strings, every supported type —
 * the {@link PositionalNdJsonDecoder} output must equal what <strong>Jackson</strong> produces for
 * the same bytes. Jackson is the oracle: it defines the semantics the positional decoder reproduces.
 *
 * <p>The test emulates the caller contract ({@link NdJsonPageDecoder}): columns the decoder did not
 * touch are {@code appendNull}'d, so a missing field and an explicit {@code null} both surface as a
 * null block value — exactly as in the live read path.
 */
public class PositionalNdJsonDecoderDifferentialTests extends ESTestCase {

    private static final Object NULL = new Object(); // explicit-JSON-null marker in the oracle

    private static final String[] NAMES = { "a_long", "b_int", "c_name", "d_ratio", "e_flag", "f_id", "g_text", "h_count" };
    private static final DataType[] TYPES = {
        DataType.LONG,
        DataType.INTEGER,
        DataType.KEYWORD,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.LONG,
        DataType.KEYWORD,
        DataType.INTEGER };
    private static final int N = NAMES.length;

    private final BlockFactory factory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);
    private final JsonFactory jsonFactory = new JsonFactory();
    private final Map<String, Integer> nameToCol = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        for (int c = 0; c < N; c++) {
            nameToCol.put(NAMES[c], c);
        }
    }

    public void testRandomizedDifferentialAgainstJackson() throws IOException {
        int iterations = scaledRandomIntBetween(500, 5000);
        for (int it = 0; it < iterations; it++) {
            // Fresh decoder each iteration so every random shape exercises the optimistic fast path
            // + per-line fallback independently (cross-line adaptation is covered elsewhere).
            PositionalNdJsonDecoder dec = new PositionalNdJsonDecoder(NAMES.clone(), TYPES.clone());
            String json = randomRecord();
            byte[] line = json.getBytes(StandardCharsets.UTF_8);

            Object[] expected = jacksonOracle(line);

            BitSet touched = new BitSet();
            Block.Builder[] builders = newBuilders();
            Block[] blocks = null;
            try {
                boolean ok = dec.tryDecodeLine(line, 0, line.length, builders, touched);
                assertTrue("in-scope record was rejected: " + json, ok);
                for (int c = 0; c < N; c++) {
                    if (touched.get(c) == false) {
                        builders[c].appendNull(); // caller contract: absent column -> null
                    }
                }
                blocks = new Block[N];
                for (int c = 0; c < N; c++) {
                    blocks[c] = builders[c].build();
                }
                for (int c = 0; c < N; c++) {
                    assertColumn(json, c, expected[c], blocks[c]);
                }
            } finally {
                if (blocks != null) {
                    Releasables.close(blocks);
                }
                Releasables.close(builders);
            }
        }
    }

    /** Parse {@code line} with Jackson into the per-column oracle: a typed value, {@link #NULL}, or {@code null} (absent). */
    private Object[] jacksonOracle(byte[] line) throws IOException {
        Object[] exp = new Object[N];
        try (JsonParser p = jsonFactory.createParser(line)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new AssertionError("generated non-object: " + new String(line, StandardCharsets.UTF_8));
            }
            String fn;
            while ((fn = p.nextFieldName()) != null) {
                JsonToken t = p.nextToken();
                Integer col = nameToCol.get(fn);
                if (col == null) {
                    p.skipChildren();
                    continue;
                }
                int c = col;
                if (t == JsonToken.VALUE_NULL) {
                    exp[c] = NULL;
                    continue;
                }
                switch (TYPES[c]) {
                    case LONG -> exp[c] = p.getLongValue();
                    case INTEGER -> exp[c] = p.getIntValue();
                    case DOUBLE -> exp[c] = p.getDoubleValue();
                    case BOOLEAN -> exp[c] = p.getBooleanValue();
                    case KEYWORD -> exp[c] = new BytesRef(p.getValueAsString());
                    default -> throw new AssertionError();
                }
            }
        }
        return exp;
    }

    /** Build one random record: a random subset of columns, in random order, plus extra unknown fields. */
    private String randomRecord() {
        List<Integer> present = new ArrayList<>();
        for (int c = 0; c < N; c++) {
            if (randomInt(4) != 0) { // ~80% present; the rest are "missing" -> oracle null
                present.add(c);
            }
        }
        Collections.shuffle(present, random()); // random field order

        List<String> frags = new ArrayList<>();
        for (int c : present) {
            String valueJson = rarely() ? "null" : randomValue(c);
            frags.add('"' + NAMES[c] + "\":" + valueJson);
        }
        // Extra unknown fields at random positions — both decoders must ignore them.
        int extras = randomInt(3);
        for (int e = 0; e < extras; e++) {
            String name = "x_" + randomAlphaOfLengthBetween(1, 6);
            if (nameToCol.containsKey(name)) {
                continue;
            }
            String v = randomBoolean() ? Integer.toString(randomInt()) : ('"' + jsonEscape(randomKeyword()) + '"');
            // randomInt(max) is inclusive: randomInt(size) yields a valid insertion index in [0, size].
            frags.add(randomInt(frags.size()), '"' + name + "\":" + v);
        }

        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < frags.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(frags.get(i));
        }
        return sb.append('}').toString();
    }

    private String randomValue(int col) {
        return switch (TYPES[col]) {
            case LONG -> Long.toString(randomLong());
            case INTEGER -> Integer.toString(randomInt());
            case DOUBLE -> Double.toString(randomDouble() * 1_000_000 - 500_000); // finite, round-trips
            case BOOLEAN -> Boolean.toString(randomBoolean());
            case KEYWORD -> '"' + jsonEscape(randomKeyword()) + '"';
            default -> throw new AssertionError("unsupported type in test schema: " + TYPES[col]);
        };
    }

    /** A random keyword that sometimes contains characters requiring JSON escaping. */
    private String randomKeyword() {
        int len = randomInt(20);
        StringBuilder s = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            switch (randomInt(20)) {
                case 0 -> s.append('"');
                case 1 -> s.append('\\');
                case 2 -> s.append('\n');
                case 3 -> s.append('\t');
                default -> s.append((char) ('a' + randomInt(26)));
            }
        }
        return s.toString();
    }

    private static String jsonEscape(String s) {
        StringBuilder b = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"' -> b.append('\\').append('"');
                case '\\' -> b.append('\\').append('\\');
                case '\n' -> b.append('\\').append('n');
                case '\t' -> b.append('\\').append('t');
                case '\r' -> b.append('\\').append('r');
                default -> b.append(ch);
            }
        }
        return b.toString();
    }

    private void assertColumn(String json, int col, Object expected, Block block) {
        if (expected == null || expected == NULL) {
            assertTrue("col " + NAMES[col] + " should be null for: " + json, block.isNull(0));
            return;
        }
        assertFalse("col " + NAMES[col] + " should be non-null for: " + json, block.isNull(0));
        switch (TYPES[col]) {
            case LONG -> assertEquals(json, ((Long) expected).longValue(), ((LongBlock) block).getLong(0));
            case INTEGER -> assertEquals(json, ((Integer) expected).intValue(), ((IntBlock) block).getInt(0));
            case DOUBLE -> assertEquals(json, (Double) expected, ((DoubleBlock) block).getDouble(0), 0.0);
            case BOOLEAN -> assertEquals(json, (Boolean) expected, ((BooleanBlock) block).getBoolean(0));
            case KEYWORD -> assertEquals(json, (BytesRef) expected, ((BytesRefBlock) block).getBytesRef(0, new BytesRef()));
            default -> throw new AssertionError();
        }
    }

    private Block.Builder[] newBuilders() {
        Block.Builder[] b = new Block.Builder[N];
        for (int c = 0; c < N; c++) {
            b[c] = switch (TYPES[c]) {
                case LONG -> factory.newLongBlockBuilder(1);
                case INTEGER -> factory.newIntBlockBuilder(1);
                case DOUBLE -> factory.newDoubleBlockBuilder(1);
                case BOOLEAN -> factory.newBooleanBlockBuilder(1);
                case KEYWORD -> factory.newBytesRefBlockBuilder(1);
                default -> throw new AssertionError();
            };
        }
        return b;
    }
}
