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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wiring correctness for the positional fast path inside {@link NdJsonPageDecoder}: the <em>same</em>
 * decoder, over the <em>same</em> bytes, must produce identical {@link Page}s whether the positional
 * path is enabled or forced off (pure Jackson). Randomized multi-line documents — varied schema
 * order, extra/missing fields, nulls, escaped strings, multiple batch sizes — under STRICT (clean)
 * and LENIENT (malformed lines injected, exercising the per-line Jackson fallback + skip). This is
 * the guarantee that the wiring changes throughput only, never results — proven here, not in a
 * performance run.
 */
public class PositionalNdJsonWiringDifferentialTests extends ESTestCase {

    private static final String[] NAMES = { "a_long", "b_int", "c_name", "d_ratio", "e_flag" };
    private static final DataType[] TYPES = {
        DataType.LONG,
        DataType.INTEGER,
        DataType.KEYWORD,
        DataType.DOUBLE,
        DataType.BOOLEAN };

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    @Override
    protected boolean enableWarningsCheck() {
        // Lenient runs intentionally emit skip-warnings (HeaderWarning) for malformed lines. This
        // suite asserts decoded rows, not warning content (which is per-doc dynamic), so opt out of
        // ESTestCase's un-asserted-warning leak check rather than pin non-deterministic warning text.
        return false;
    }

    public void testStrictCleanDocsMatchJackson() throws IOException {
        int docs = scaledRandomIntBetween(20, 200);
        for (int d = 0; d < docs; d++) {
            byte[] bytes = randomDocument(randomIntBetween(1, 40), false).getBytes(StandardCharsets.UTF_8);
            assertWiringMatchesJackson(bytes, randomFrom(1, 4, 16, 1024), ErrorPolicy.STRICT);
        }
    }

    public void testLenientWithMalformedLinesMatchJackson() throws IOException {
        int docs = scaledRandomIntBetween(20, 200);
        for (int d = 0; d < docs; d++) {
            byte[] bytes = randomDocument(randomIntBetween(1, 40), true).getBytes(StandardCharsets.UTF_8);
            assertWiringMatchesJackson(bytes, randomFrom(1, 4, 16, 1024), ErrorPolicy.LENIENT);
        }
    }

    private void assertWiringMatchesJackson(byte[] bytes, int batchSize, ErrorPolicy policy) throws IOException {
        List<Object[]> positional = drainRows(bytes, batchSize, policy, true);
        List<Object[]> jackson = drainRows(bytes, batchSize, policy, false);
        String doc = new String(bytes, StandardCharsets.UTF_8);
        assertEquals("row count differs (batch=" + batchSize + ") for doc:\n" + doc, jackson.size(), positional.size());
        for (int i = 0; i < jackson.size(); i++) {
            assertArrayEquals("row " + i + " differs (batch=" + batchSize + ") for doc:\n" + doc, jackson.get(i), positional.get(i));
        }
    }

    private List<Object[]> drainRows(byte[] bytes, int batchSize, ErrorPolicy policy, boolean positional) throws IOException {
        List<Object[]> rows = new ArrayList<>();
        try (
            NdJsonPageDecoder dec = new NdJsonPageDecoder(
                bytes,
                0,
                bytes.length,
                attributes(),
                null,
                batchSize,
                blockFactory,
                policy,
                "test://wiring"
            )
        ) {
            if (positional == false) {
                dec.setPositionalEnabled(false);
            }
            while (true) {
                Page page = dec.decodePage();
                if (page == null) {
                    break;
                }
                try (page) {
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        Object[] row = new Object[NAMES.length];
                        for (int c = 0; c < NAMES.length; c++) {
                            row[c] = value(page.getBlock(c), p);
                        }
                        rows.add(row);
                    }
                }
            }
        }
        return rows;
    }

    private static Object value(Block b, int pos) {
        if (b.isNull(pos)) {
            return null;
        }
        if (b instanceof LongBlock x) {
            return x.getLong(pos);
        }
        if (b instanceof IntBlock x) {
            return x.getInt(pos);
        }
        if (b instanceof DoubleBlock x) {
            return x.getDouble(pos);
        }
        if (b instanceof BooleanBlock x) {
            return x.getBoolean(pos);
        }
        if (b instanceof BytesRefBlock x) {
            return BytesRef.deepCopyOf(x.getBytesRef(pos, new BytesRef()));
        }
        throw new AssertionError("unexpected block type: " + b.getClass());
    }

    private static List<Attribute> attributes() {
        List<Attribute> attrs = new ArrayList<>(NAMES.length);
        for (int c = 0; c < NAMES.length; c++) {
            attrs.add(NdJsonSchemaInferrer.attribute(NAMES[c], TYPES[c], true));
        }
        return attrs;
    }

    private String randomDocument(int rows, boolean allowMalformed) {
        StringBuilder sb = new StringBuilder();
        for (int r = 0; r < rows; r++) {
            sb.append(allowMalformed && rarely() ? randomMalformedLine() : randomRecord());
            sb.append('\n');
        }
        return sb.toString();
    }

    private String randomMalformedLine() {
        // All of these error WITHIN their own line, so both the positional fallback and the streaming
        // Jackson path recover at the same line boundary. An unterminated object (no closing brace) is
        // deliberately excluded here: it makes the streaming path error on the NEXT line's '{' and
        // skip that innocent line too, whereas positional recovers per line and keeps it — a legitimate
        // (positional-favorable) difference covered by testUnterminatedLineRecoversPerLine instead.
        return randomFrom(
            "{\"a_long\":}",                       // missing value
            "not json at all",                     // not an object
            "{\"c_name\":\"x\" \"e_flag\":true}",   // missing comma
            "{\"a_long\":1,}",                      // trailing comma
            "{\"d_ratio\":1.2.3}"                   // malformed number
        );
    }

    /**
     * An unterminated line is skipped, and — unlike the streaming Jackson path, whose error recovery
     * over-consumes the following line — the positional path recovers per line and keeps the clean
     * line after it. Asserts the two clean rows survive with correct values.
     */
    public void testUnterminatedLineRecoversPerLine() throws IOException {
        String doc = "{\"a_long\":10,\"b_int\":20,\"c_name\":\"x\",\"d_ratio\":1.5,\"e_flag\":true}\n"
            + "{\"b_int\":1\n" // unterminated: no closing brace
            + "{\"a_long\":30,\"b_int\":40,\"c_name\":\"y\",\"d_ratio\":2.5,\"e_flag\":false}\n";
        List<Object[]> rows = drainRows(doc.getBytes(StandardCharsets.UTF_8), 1024, ErrorPolicy.LENIENT, true);
        assertEquals("both clean lines must survive; only the unterminated line is skipped", 2, rows.size());
        assertEquals(10L, rows.get(0)[0]);
        assertEquals(new BytesRef("x"), rows.get(0)[2]);
        assertEquals(30L, rows.get(1)[0]);
        assertEquals(new BytesRef("y"), rows.get(1)[2]);
    }

    private String randomRecord() {
        List<Integer> present = new ArrayList<>();
        for (int c = 0; c < NAMES.length; c++) {
            if (randomInt(4) != 0) {
                present.add(c);
            }
        }
        Collections.shuffle(present, random());
        List<String> frags = new ArrayList<>();
        for (int c : present) {
            frags.add('"' + NAMES[c] + "\":" + (rarely() ? "null" : randomValue(c)));
        }
        for (int e = 0, extras = randomInt(2); e < extras; e++) {
            frags.add(randomInt(frags.size()), "\"x_" + randomAlphaOfLengthBetween(1, 5) + "\":" + randomInt());
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
            case DOUBLE -> Double.toString(randomDouble() * 1_000_000 - 500_000);
            case BOOLEAN -> Boolean.toString(randomBoolean());
            case KEYWORD -> '"' + jsonEscape(randomKeyword()) + '"';
            default -> throw new AssertionError();
        };
    }

    private String randomKeyword() {
        int len = randomInt(16);
        StringBuilder s = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            switch (randomInt(16)) {
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
}
