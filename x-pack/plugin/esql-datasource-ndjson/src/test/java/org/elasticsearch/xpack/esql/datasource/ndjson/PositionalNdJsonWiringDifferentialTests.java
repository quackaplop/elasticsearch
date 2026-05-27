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
    private static final DataType[] TYPES = { DataType.LONG, DataType.INTEGER, DataType.KEYWORD, DataType.DOUBLE, DataType.BOOLEAN };

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

    /**
     * Raw bytes Jackson rejects — invalid UTF-8 lead/continuation/truncation, an unescaped control
     * byte, a lone low surrogate — must make positional defer (so both paths skip the line under
     * lenient), while a valid multi-byte string (Cyrillic) survives in both. Proves the UTF-8
     * validation matches Jackson and never crashes on adversarial bytes.
     */
    private static final int INVALID_UTF8_LEAD = 0xFF;       // not a valid UTF-8 lead byte
    private static final int LONE_CONTINUATION_BYTE = 0x80;   // continuation byte with no lead
    private static final int TRUNCATED_2BYTE_LEAD = 0xC3;     // 2-byte lead with no continuation following
    private static final int UNESCAPED_CONTROL_BYTE = 0x01;   // raw control char (JSON requires it escaped)
    private static final String LONE_LOW_SURROGATE_ESCAPE = "\\" + "uDC00"; // JSON escape for an unpaired low surrogate
    private static final String CYRILLIC_VALID = "да";        // valid multi-byte UTF-8 that must be accepted

    public void testRawInvalidUtf8AndControlBytesMatchJackson() throws IOException {
        java.io.ByteArrayOutputStream doc = new java.io.ByteArrayOutputStream();
        appendUtf8(doc, "{\"a_long\":1,\"b_int\":2,\"c_name\":\"ok\",\"d_ratio\":1.5,\"e_flag\":true}\n");
        appendBadStringLine(doc, INVALID_UTF8_LEAD);
        appendBadStringLine(doc, LONE_CONTINUATION_BYTE);
        appendBadStringLine(doc, TRUNCATED_2BYTE_LEAD);
        appendBadStringLine(doc, UNESCAPED_CONTROL_BYTE);
        appendUtf8(doc, "{\"c_name\":\"" + LONE_LOW_SURROGATE_ESCAPE + "\"}\n");
        appendUtf8(doc, "{\"a_long\":9,\"c_name\":\"" + CYRILLIC_VALID + "\"}\n"); // valid Cyrillic must survive
        appendUtf8(doc, "{\"a_long\":3,\"b_int\":4,\"c_name\":\"end\",\"d_ratio\":2.5,\"e_flag\":false}\n");
        assertWiringMatchesJackson(doc.toByteArray(), randomFrom(1, 4, 1024), ErrorPolicy.LENIENT);
    }

    /**
     * Heavy fuzz of the hand-rolled UTF-8 / string-content validation against Jackson. Each iteration
     * builds a single {@code {"c_name":"<random bytes>"}} line whose value is an adversarial mix of
     * valid code points (incl. boundary + astral), lone continuation bytes, invalid leads, truncated
     * and overlong sequences, surrogate ranges, control chars and raw garbage. Positional must decode
     * exactly what Jackson decodes or defer to it (single-line docs avoid lenient over-consume recovery
     * differences), and must never throw. Thousands of cases per run, seed-reproducible.
     */
    public void testUtf8StringContentFuzzMatchesJackson() throws IOException {
        int iterations = scaledRandomIntBetween(2000, 20000);
        for (int it = 0; it < iterations; it++) {
            java.io.ByteArrayOutputStream line = new java.io.ByteArrayOutputStream();
            appendUtf8(line, "{\"a_long\":7,\"c_name\":\"");
            appendRandomStringContent(line);
            appendUtf8(line, "\"}\n");
            assertWiringMatchesJackson(line.toByteArray(), randomFrom(1, 1024), ErrorPolicy.LENIENT);
        }
    }

    /** Append an adversarial byte mix to a JSON string value (no raw newline, which would split the line). */
    private void appendRandomStringContent(java.io.ByteArrayOutputStream out) {
        int parts = randomInt(10);
        for (int i = 0; i < parts; i++) {
            switch (randomInt(7)) {
                case 0, 1 -> { // valid code point (weighted common): exercises the accept path incl. multi-byte
                    byte[] u = new String(Character.toChars(randomValidCodePoint())).getBytes(StandardCharsets.UTF_8);
                    out.write(u, 0, u.length);
                }
                case 2 -> out.write(0x80 + randomInt(0x40));                       // lone continuation byte
                case 3 -> out.write(randomFrom(0xC0, 0xC1, 0xF5, 0xF6, 0xFE, 0xFF)); // always-invalid lead byte
                case 4 -> out.write(0xC2 + randomInt(0x1E));                       // multi-byte lead (often truncated)
                case 5 -> writeNonNewline(out, randomInt(0x20));                   // control character
                default -> writeNonNewline(out, randomInt(0x100));                 // raw byte
            }
        }
    }

    private static void writeNonNewline(java.io.ByteArrayOutputStream out, int b) {
        if (b != '\n' && b != '\r') { // a raw newline would split the line; line-splitting isn't under test here
            out.write(b);
        }
    }

    private int randomValidCodePoint() {
        while (true) {
            int cp = randomIntBetween(0x20, 0x10FFFF); // printable range
            if (cp < 0xD800 || cp > 0xDFFF) {          // exclude UTF-16 surrogates (not valid scalar values)
                return cp;
            }
        }
    }

    /** A {@code c_name} string line with {@code rawByte} embedded mid-value — a byte Jackson rejects. */
    private void appendBadStringLine(java.io.ByteArrayOutputStream doc, int rawByte) {
        appendUtf8(doc, "{\"c_name\":\"a");
        doc.write(rawByte);
        appendUtf8(doc, "b\"}\n");
    }

    private static void appendUtf8(java.io.ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.write(b, 0, b.length);
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
                "test://wiring",
                new NdJsonReaderCounters()
            )
        ) {
            dec.setPositionalEnabled(positional); // explicit: positional default is now opt-in (off)
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
        int count = b.getValueCount(pos);
        int first = b.getFirstValueIndex(pos);
        if (count == 1) {
            return scalarAt(b, first);
        }
        List<Object> vals = new ArrayList<>(count); // multi-value (array values reached via Jackson fallback)
        for (int i = 0; i < count; i++) {
            vals.add(scalarAt(b, first + i));
        }
        return vals;
    }

    private static Object scalarAt(Block b, int valueIndex) {
        if (b instanceof LongBlock x) {
            return x.getLong(valueIndex);
        }
        if (b instanceof IntBlock x) {
            return x.getInt(valueIndex);
        }
        if (b instanceof DoubleBlock x) {
            return x.getDouble(valueIndex);
        }
        if (b instanceof BooleanBlock x) {
            return x.getBoolean(valueIndex);
        }
        if (b instanceof BytesRefBlock x) {
            return BytesRef.deepCopyOf(x.getBytesRef(valueIndex, new BytesRef()));
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
            "{\"d_ratio\":1.2.3}",                  // malformed number
            "{\"a_long\":007}",                     // leading zeros (canonical-JSON reject)
            "{\"b_int\":+5}",                       // leading plus (canonical-JSON reject)
            "{\"d_ratio\":.5}",                     // leading dot (canonical-JSON reject)
            "{\"a_long\":-}",                       // lone minus
            "{\"d_ratio\":1e}",                     // empty exponent
            "{\"a_long\":99999999999999999999999}"  // overflows long range
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
        // ~1 in 9 values is an array: positional defers on '[' and Jackson decodes it to a multi-value
        // block, so this exercises the defer-to-success fallback path (must still match pure Jackson).
        if (randomInt(8) == 0) {
            return "[" + scalarJson(col) + "," + scalarJson(col) + "]";
        }
        return scalarJson(col);
    }

    private String scalarJson(int col) {
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
