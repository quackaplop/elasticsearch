/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Schema-positional NDJSON line decoder — the {@code elastic/esql-planning#710} fast path that
 * bypasses Jackson's general-purpose tokenizer. Given the projected columns' names and types
 * (known at planning time), it walks one line's raw bytes and writes typed values straight into
 * the per-column {@link Block.Builder}s, with no token stream, no field-name interning, and no
 * {@code String} allocation for keyword values (the common, unescaped case copies bytes directly).
 *
 * <p><strong>Scope of this first increment.</strong> This handles the common ClickBench-style
 * shape: a flat object of scalar fields ({@code long}/{@code int}/{@code double}/{@code boolean}/
 * keyword/{@code datetime}/{@code null}), fields in any order. Anything outside that — arrays,
 * nested objects, or a value whose shape doesn't match the column's declared type — makes
 * {@link #tryDecodeLine} return {@code false} so the caller falls
 * back to Jackson for that single line ({@link NdJsonPageDecoder}'s long-tail path). Wiring this
 * into the live decode loop is deferred until the in-flight aggregate-metadata-pushdown work
 * (elastic/elasticsearch#149380) merges, since both touch {@link NdJsonPageDecoder}.
 *
 * <p>Not thread-safe: one instance per consumer (it owns mutable scan + unescape scratch).
 */
public final class PositionalNdJsonDecoder {

    /** Sentinel returned by {@link #lookup} for a field name not in the projected schema. */
    private static final int UNKNOWN = -1;

    private final DataType[] types;

    // Field-name resolution is two-tier. Fast path: confirm the next field is the column we expect
    // at this position with one byte-compare (no hashing) — this is the win that makes the decoder
    // beat Jackson on x86, where hashing every field name only matched it. Fallback: an
    // open-addressing hash (field-name bytes -> column index) handles fields that aren't where the
    // positional plan expects them, order-independent per the NDJSON spec.
    private final int mask;
    private final int[] slotCol;
    private final byte[][] nameBytes;

    // Reusable scratch so keyword decode allocates nothing on the steady-state path.
    private byte[] strScratch = new byte[256];
    private final BytesRef scratchRef = new BytesRef();

    private int pos; // scan cursor, reset per line

    // ---- adaptive positional plan ----
    // `order` is the expected sequence of projected-column indices; a per-line cursor walks it and
    // confirms each entry with one byte-compare. It starts in schema order (the common case: fields
    // arrive in declared order). NDJSON does NOT require a fixed field order, so we adapt: if a
    // window of lines shows a low fast-path hit rate — a consistently reordered file — we re-learn
    // `order` from the most recent line's observed sequence. If a second window is still poor (e.g.
    // the order changes every line and is unlearnable), we disable the fast path and run pure-hash,
    // which bounds the worst case to the hash decoder's speed with no re-learn thrash. Correctness
    // is independent of all this: every resolution is verified by name (fast compare or hash), so a
    // wrong positional guess only costs a hash, never a wrong column. A single out-of-order line
    // cannot trigger a re-learn — only a sustained low hit rate over the whole window can.
    private final int[] order; // expected projected-column sequence; valid entries [0, orderLen)
    private int orderLen;
    private final int[] seen;  // scratch: projected-column sequence observed on the current line
    private boolean fastPathEnabled = true;
    private int relearnAttempts;
    private int windowLines;
    private int windowFastHits;
    private int windowProjected;

    private static final int ADAPT_WINDOW_LINES = 32;

    /**
     * @param fieldNames projected column names, in block order
     * @param types      projected column types, parallel to {@code fieldNames}
     */
    public PositionalNdJsonDecoder(String[] fieldNames, DataType[] types) {
        if (fieldNames.length != types.length) {
            throw new IllegalArgumentException("fieldNames/types length mismatch");
        }
        this.types = types.clone();
        int n = fieldNames.length;
        int cap = Integer.highestOneBit(Math.max(4, n * 4 - 1)) << 1; // pow2, load < 0.5
        this.mask = cap - 1;
        this.slotCol = new int[cap];
        Arrays.fill(slotCol, UNKNOWN);
        this.nameBytes = new byte[n][];
        for (int c = 0; c < n; c++) {
            byte[] nb = fieldNames[c].getBytes(StandardCharsets.UTF_8);
            nameBytes[c] = nb;
            int idx = hash(nb, 0, nb.length) & mask;
            while (slotCol[idx] != UNKNOWN) {
                idx = (idx + 1) & mask;
            }
            slotCol[idx] = c;
        }
        // Positional plan starts in schema order; adapts per the windowed policy in adaptAfterLine.
        this.order = new int[n];
        for (int c = 0; c < n; c++) {
            order[c] = c;
        }
        this.orderLen = n;
        this.seen = new int[n];
    }

    /**
     * Decode one NDJSON object from {@code buf[from, to)} into {@code builders}, recording which
     * columns received a value in {@code touched} (the caller appends {@code null} to the rest, as
     * {@link NdJsonPageDecoder} does via its block tracker).
     *
     * @return {@code true} if the whole line was decoded on the fast path; {@code false} if the
     *         caller must fall back to Jackson for this line (nothing is appended on a
     *         {@code false} return — the method bails before the first append it can't complete).
     */
    public boolean tryDecodeLine(byte[] buf, int from, int to, Block.Builder[] builders, java.util.BitSet touched) {
        pos = from;
        skipWs(buf, to);
        if (pos >= to || buf[pos] != '{') {
            return false;
        }
        pos++;
        skipWs(buf, to);
        if (pos < to && buf[pos] == '}') {
            return true; // empty object (no projected fields; caller fills nulls)
        }
        int oc = 0;        // cursor into `order` (the positional plan)
        int seenCount = 0; // projected columns resolved this line, in arrival order
        int fastHits = 0;  // resolved via the positional byte-compare rather than the hash
        int projected = 0; // projected columns resolved this line (fast or hash)
        while (true) {
            skipWs(buf, to);
            if (pos >= to || buf[pos] != '"') {
                return false;
            }
            int ns = ++pos;
            // Field names are matched on raw bytes; an escaped field name is rare enough to punt to Jackson.
            while (pos < to && buf[pos] != '"') {
                if (buf[pos] == '\\') {
                    return false;
                }
                pos++;
            }
            if (pos >= to) {
                return false;
            }
            int nlen = pos - ns;
            pos++; // closing name quote
            skipWs(buf, to);
            if (pos >= to || buf[pos] != ':') {
                return false;
            }
            pos++;
            skipWs(buf, to);

            // Positional fast path: expect order[oc] next, confirm with one byte-compare (no hashing).
            int col;
            boolean fastHit = false;
            if (fastPathEnabled && oc < orderLen) {
                int cand = order[oc];
                if (nameBytes[cand].length == nlen && bytesEqual(buf, ns, nameBytes[cand])) {
                    col = cand;
                    oc++;
                    fastHit = true;
                } else {
                    col = lookup(buf, ns, nlen); // not where expected -> hash fallback
                }
            } else {
                col = lookup(buf, ns, nlen);
            }
            if (decodeValue(buf, to, col, col == UNKNOWN ? null : builders[col]) == false) {
                return false;
            }
            if (col != UNKNOWN) {
                touched.set(col);
                if (seenCount < seen.length) {
                    seen[seenCount++] = col;
                }
                projected++;
                if (fastHit) {
                    fastHits++;
                }
            }

            skipWs(buf, to);
            if (pos >= to) {
                return false;
            }
            byte ch = buf[pos];
            if (ch == ',') {
                pos++;
                continue;
            }
            if (ch == '}') {
                adaptAfterLine(projected, fastHits, seenCount);
                return true;
            }
            return false;
        }
    }

    /**
     * Windowed adaptation of the positional plan. Accumulates the fast-path hit rate over
     * {@link #ADAPT_WINDOW_LINES} fully-decoded lines, then: if the rate is healthy, does nothing
     * (the common case — fields arrive in the expected order); if it is poor, re-learns {@link #order}
     * from the most recent line's observed sequence once; if it is still poor after that re-learn,
     * disables the fast path so the rest of the page runs pure-hash without re-learn thrash. A single
     * out-of-order line cannot trip this — only a sustained low rate across the whole window can.
     */
    private void adaptAfterLine(int projected, int fastHits, int seenCount) {
        if (fastPathEnabled == false) {
            return; // already pure-hash; no plan to tune
        }
        windowFastHits += fastHits;
        windowProjected += projected;
        windowLines++;
        if (windowLines < ADAPT_WINDOW_LINES) {
            return;
        }
        boolean poor = windowProjected > 0 && windowFastHits * 2 < windowProjected; // hit rate < 50%
        if (poor) {
            if (relearnAttempts == 0 && seenCount > 0) {
                System.arraycopy(seen, 0, order, 0, seenCount); // adopt this line's observed order
                orderLen = seenCount;
                relearnAttempts = 1;
            } else {
                fastPathEnabled = false; // re-learn didn't help -> stop guessing, run pure-hash
            }
        } else {
            relearnAttempts = 0; // healthy window clears the strike
        }
        windowLines = 0;
        windowFastHits = 0;
        windowProjected = 0;
    }

    // visible for testing: the adaptive plan's current state.
    boolean isFastPathEnabled() {
        return fastPathEnabled;
    }

    int[] expectedOrder() {
        return Arrays.copyOf(order, orderLen);
    }

    /** Decode the value at {@link #pos} into {@code builder} (or skip it if {@code col == UNKNOWN}). */
    private boolean decodeValue(byte[] b, int to, int col, Block.Builder builder) {
        byte c = b[pos];
        // Arrays and nested objects are out of fast-path scope.
        if (c == '[' || c == '{') {
            return false;
        }
        if (c == 'n') { // null literal
            if (regionEquals(b, to, "null") == false) {
                return false;
            }
            pos += 4;
            // Matches NdJsonPageDecoder: an explicit JSON null is appended inline (and the caller
            // marks the column touched). Only fields ABSENT from the line are filled by the caller's
            // end-of-line appendNull. A null for an unprojected column is simply skipped.
            if (col != UNKNOWN) {
                builder.appendNull();
            }
            return true;
        }
        if (col == UNKNOWN) {
            return skipScalar(b, to);
        }
        DataType type = types[col];
        if (c == '"') {
            // String-shaped value. Valid for KEYWORD (raw/unescaped bytes) and DATETIME (parse to millis).
            int valEnd = scanStringEnd(b, to); // pos left at opening quote+1 region via fields below
            if (valEnd < 0) {
                return false;
            }
            // strScratch[0..strLen) holds the unescaped UTF-8 bytes; pos advanced past closing quote.
            int strLen = lastStrLen;
            switch (type) {
                case KEYWORD -> {
                    scratchRef.bytes = strScratch;
                    scratchRef.offset = 0;
                    scratchRef.length = strLen;
                    ((BytesRefBlock.Builder) builder).appendBytesRef(scratchRef);
                    return true;
                }
                case DATETIME -> {
                    try {
                        String s = new String(strScratch, 0, strLen, StandardCharsets.UTF_8);
                        long millis = NdJsonSchemaInferrer.DATE_FORMATTER.parseMillis(s);
                        ((LongBlock.Builder) builder).appendLong(millis);
                        return true;
                    } catch (Exception e) {
                        return false; // unparseable date -> Jackson's lenient path decides
                    }
                }
                default -> {
                    return false; // string value for a numeric/boolean column -> coercion, punt
                }
            }
        }
        if (c == 't' || c == 'f') { // boolean
            if (type != DataType.BOOLEAN) {
                return false;
            }
            if (c == 't' && regionEquals(b, to, "true")) {
                pos += 4;
                ((BooleanBlock.Builder) builder).appendBoolean(true);
                return true;
            }
            if (c == 'f' && regionEquals(b, to, "false")) {
                pos += 5;
                ((BooleanBlock.Builder) builder).appendBoolean(false);
                return true;
            }
            return false;
        }
        // number
        int numStart = pos;
        boolean fractional = scanNumber(b, to);
        int numEnd = pos;
        if (numEnd == numStart) {
            return false; // a value was expected but there's no number here (malformed) -> let Jackson decide
        }
        switch (type) {
            case INTEGER -> {
                if (fractional) {
                    return false;
                }
                long v = parseLong(b, numStart, numEnd);
                if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                    return false; // coercion overflow -> Jackson path
                }
                ((IntBlock.Builder) builder).appendInt((int) v);
                return true;
            }
            case LONG -> {
                if (fractional) {
                    return false;
                }
                ((LongBlock.Builder) builder).appendLong(parseLong(b, numStart, numEnd));
                return true;
            }
            case DOUBLE -> {
                // Use the JDK parser on the exact token so the value matches Jackson bit-for-bit.
                try {
                    double d = Double.parseDouble(new String(b, numStart, numEnd - numStart, StandardCharsets.US_ASCII));
                    ((DoubleBlock.Builder) builder).appendDouble(d);
                    return true;
                } catch (NumberFormatException e) {
                    return false; // malformed number -> defer to Jackson rather than fabricate or throw
                }
            }
            default -> {
                return false; // number for a string/boolean column -> coercion, punt
            }
        }
    }

    // ---- scanning helpers (operate on the instance cursor `pos`) ----

    private int lastStrLen;

    /**
     * Decode the JSON string starting at {@link #pos} (the opening quote) into {@link #strScratch},
     * setting {@link #lastStrLen}, and advance {@code pos} past the closing quote.
     *
     * @return the index just after the closing quote, or {@code -1} if malformed.
     */
    private int scanStringEnd(byte[] b, int to) {
        int i = pos + 1; // past opening quote
        int out = 0;
        byte[] scratch = strScratch;
        while (i < to) {
            byte ch = b[i];
            if (ch == '"') {
                lastStrLen = out;
                strScratch = scratch;
                pos = i + 1;
                return pos;
            }
            if (ch == '\\') {
                i++;
                if (i >= to) {
                    return -1;
                }
                byte esc = b[i++];
                int cp;
                switch (esc) {
                    case '"' -> cp = '"';
                    case '\\' -> cp = '\\';
                    case '/' -> cp = '/';
                    case 'b' -> cp = '\b';
                    case 'f' -> cp = '\f';
                    case 'n' -> cp = '\n';
                    case 'r' -> cp = '\r';
                    case 't' -> cp = '\t';
                    case 'u' -> {
                        if (i + 4 > to) {
                            return -1;
                        }
                        int u = hex4(b, i);
                        if (u < 0) {
                            return -1;
                        }
                        i += 4;
                        if (u >= 0xD800 && u <= 0xDBFF) { // high surrogate -> expect a low-surrogate escape next
                            if (i + 6 > to || b[i] != '\\' || b[i + 1] != 'u') {
                                return -1;
                            }
                            int lo = hex4(b, i + 2);
                            if (lo < 0xDC00 || lo > 0xDFFF) {
                                return -1;
                            }
                            i += 6;
                            cp = 0x10000 + ((u - 0xD800) << 10) + (lo - 0xDC00);
                        } else {
                            cp = u;
                        }
                    }
                    default -> {
                        return -1;
                    }
                }
                scratch = ensure(scratch, out + 4);
                out = appendUtf8(scratch, out, cp);
                continue;
            }
            // raw byte (already UTF-8 on the wire)
            scratch = ensure(scratch, out + 1);
            scratch[out++] = ch;
            i++;
        }
        return -1; // unterminated
    }

    private byte[] ensure(byte[] a, int need) {
        if (need <= a.length) {
            return a;
        }
        byte[] grown = new byte[Math.max(a.length * 2, need)];
        System.arraycopy(a, 0, grown, 0, a.length);
        strScratch = grown;
        return grown;
    }

    private static int appendUtf8(byte[] out, int p, int cp) {
        if (cp < 0x80) {
            out[p++] = (byte) cp;
        } else if (cp < 0x800) {
            out[p++] = (byte) (0xC0 | (cp >> 6));
            out[p++] = (byte) (0x80 | (cp & 0x3F));
        } else if (cp < 0x10000) {
            out[p++] = (byte) (0xE0 | (cp >> 12));
            out[p++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
            out[p++] = (byte) (0x80 | (cp & 0x3F));
        } else {
            out[p++] = (byte) (0xF0 | (cp >> 18));
            out[p++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
            out[p++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
            out[p++] = (byte) (0x80 | (cp & 0x3F));
        }
        return p;
    }

    private static int hex4(byte[] b, int i) {
        int v = 0;
        for (int k = 0; k < 4; k++) {
            int d = Character.digit(b[i + k], 16);
            if (d < 0) {
                return -1;
            }
            v = (v << 4) | d;
        }
        return v;
    }

    /** Skip a scalar value at {@link #pos} (for unprojected fields). */
    private boolean skipScalar(byte[] b, int to) {
        byte c = b[pos];
        if (c == '"') {
            return scanStringEnd(b, to) >= 0;
        }
        if (c == 't') {
            if (regionEquals(b, to, "true")) {
                pos += 4;
                return true;
            }
            return false;
        }
        if (c == 'f') {
            if (regionEquals(b, to, "false")) {
                pos += 5;
                return true;
            }
            return false;
        }
        scanNumber(b, to);
        return true;
    }

    private boolean scanNumber(byte[] b, int to) {
        boolean fractional = false;
        if (pos < to && (b[pos] == '-' || b[pos] == '+')) {
            pos++;
        }
        while (pos < to) {
            byte c = b[pos];
            if (c >= '0' && c <= '9') {
                pos++;
            } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                fractional = true;
                pos++;
            } else {
                break;
            }
        }
        return fractional;
    }

    private static long parseLong(byte[] b, int from, int to) {
        int i = from;
        boolean neg = false;
        if (b[i] == '-') {
            neg = true;
            i++;
        } else if (b[i] == '+') {
            i++;
        }
        long v = 0;
        while (i < to) {
            v = v * 10 + (b[i++] - '0');
        }
        return neg ? -v : v;
    }

    private void skipWs(byte[] b, int to) {
        while (pos < to) {
            byte c = b[pos];
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                pos++;
            } else {
                break;
            }
        }
    }

    private boolean regionEquals(byte[] b, int to, String lit) {
        if (pos + lit.length() > to) {
            return false;
        }
        for (int k = 0; k < lit.length(); k++) {
            if (b[pos + k] != (byte) lit.charAt(k)) {
                return false;
            }
        }
        return true;
    }

    private int lookup(byte[] b, int start, int len) {
        int idx = hash(b, start, len) & mask;
        int col;
        while ((col = slotCol[idx]) != UNKNOWN) {
            byte[] nb = nameBytes[col];
            if (nb.length == len && bytesEqual(b, start, nb)) {
                return col;
            }
            idx = (idx + 1) & mask;
        }
        return UNKNOWN;
    }

    private static boolean bytesEqual(byte[] b, int start, byte[] name) {
        for (int i = 0; i < name.length; i++) {
            if (b[start + i] != name[i]) {
                return false;
            }
        }
        return true;
    }

    private static int hash(byte[] b, int start, int len) {
        int h = 0x811c9dc5;
        int e = start + len;
        for (int i = start; i < e; i++) {
            h = (h ^ b[i]) * 0x01000193;
        }
        return h ^ (h >>> 16);
    }
}
