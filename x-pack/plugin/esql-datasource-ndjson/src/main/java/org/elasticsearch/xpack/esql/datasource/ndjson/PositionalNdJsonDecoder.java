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
 * {@link #tryDecodeLine} return {@code false} so the caller falls back to Jackson for that single
 * line ({@link NdJsonPageDecoder}'s long-tail path). Wired into {@link NdJsonPageDecoder}'s
 * byte-array decode path behind the {@code esql.datasource.ndjson.positional_decoding} node setting
 * (opt-in, default off): when disabled the reader is pure Jackson, unchanged.
 *
 * <p>Not thread-safe: one instance per consumer (it owns mutable scan + unescape scratch).
 */
public final class PositionalNdJsonDecoder {

    /** Sentinel returned by {@link #lookup} for a field name not in the projected schema. */
    private static final int UNKNOWN = -1;

    // String-scanner byte/codepoint constants (Unicode well-formed UTF-8 byte ranges + surrogate bounds).
    private static final int CONTROL_CHAR_LIMIT = 0x20; // bytes below this are JSON control chars (must be escaped)
    private static final int ASCII_LIMIT = 0x80;        // bytes below this are single-byte ASCII
    private static final int UTF8_CONT_MIN = 0x80, UTF8_CONT_MAX = 0xBF; // continuation byte range
    private static final int UTF8_LEAD_2B_MIN = 0xC2, UTF8_LEAD_2B_MAX = 0xDF;
    private static final int UTF8_LEAD_3B_MIN = 0xE0, UTF8_LEAD_3B_MAX = 0xEF;
    private static final int UTF8_LEAD_4B_MIN = 0xF0, UTF8_LEAD_4B_MAX = 0xF4;
    private static final int UTF8_LEAD_E0 = 0xE0, UTF8_LEAD_ED = 0xED, UTF8_LEAD_F0 = 0xF0, UTF8_LEAD_F4 = 0xF4;
    private static final int UTF8_E0_MIN_2ND = 0xA0; // E0 lead: 2nd byte >= A0 rejects overlong encodings
    private static final int UTF8_ED_MAX_2ND = 0x9F; // ED lead: 2nd byte <= 9F rejects UTF-16 surrogates
    private static final int UTF8_F0_MIN_2ND = 0x90; // F0 lead: 2nd byte >= 90 rejects overlong encodings
    private static final int UTF8_F4_MAX_2ND = 0x8F; // F4 lead: 2nd byte <= 8F rejects code points > U+10FFFF
    private static final int HIGH_SURROGATE_MIN = 0xD800, HIGH_SURROGATE_MAX = 0xDBFF;
    private static final int LOW_SURROGATE_MIN = 0xDC00, LOW_SURROGATE_MAX = 0xDFFF;
    private static final int SUPPLEMENTARY_BASE = 0x10000;

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

    // ---- per-line value slots (atomicity + speed) ----
    // decodeValue writes the decoded value into these reusable per-column slots; flush() copies the
    // slots into the page builders ONLY on a fully successful line. So a line that bails mid-decode
    // appends nothing (never-corrupt invariant), AND the common success path appends straight to the
    // page builders with no per-row scratch BlockBuilder churn (that churn made the wired path slower
    // than Jackson — this is the fix). No per-row allocation: the arrays + byte buffer are reused.
    private static final byte SLOT_UNSET = 0, SLOT_VALUE = 1, SLOT_NULL = 2;
    private final byte[] slotState;
    private final long[] longSlot;    // LONG + DATETIME (millis)
    private final int[] intSlot;      // INTEGER
    private final double[] dblSlot;   // DOUBLE
    private final boolean[] boolSlot; // BOOLEAN
    private final int[] strOff;       // KEYWORD: offset into strScratch
    private final int[] strLen;       // KEYWORD: length in strScratch
    private int strCursor;            // write position in strScratch, reset per line

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
        this.slotState = new byte[n];
        this.longSlot = new long[n];
        this.intSlot = new int[n];
        this.dblSlot = new double[n];
        this.boolSlot = new boolean[n];
        this.strOff = new int[n];
        this.strLen = new int[n];
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
        Arrays.fill(slotState, SLOT_UNSET); // reset per-line slots; values land here, flush on success
        strCursor = 0;
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
            if (decodeValue(buf, to, col) == false) {
                return false; // nothing flushed yet -> no partial append
            }
            if (col != UNKNOWN) {
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
                flush(builders, touched); // line fully decoded -> commit all slots to the page builders
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

    /** Decode the value at {@link #pos} into the per-column slot {@code col} (or skip it if UNKNOWN). */
    private boolean decodeValue(byte[] b, int to, int col) {
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
            // An explicit JSON null is appended inline at flush (matches NdJsonPageDecoder); only
            // fields ABSENT from the line are filled by the caller's end-of-line appendNull. A null
            // for an unprojected column is simply skipped.
            if (col != UNKNOWN) {
                slotState[col] = SLOT_NULL;
            }
            return true;
        }
        if (col == UNKNOWN) {
            return skipScalar(b, to);
        }
        DataType type = types[col];
        if (c == '"') {
            // String-shaped value. Valid for KEYWORD (raw/unescaped bytes) and DATETIME (parse to millis).
            // scanStringEnd decodes into strScratch[strCursor, strCursor+lastStrLen).
            int valEnd = scanStringEnd(b, to);
            if (valEnd < 0) {
                return false;
            }
            switch (type) {
                case KEYWORD -> {
                    strOff[col] = strCursor;
                    strLen[col] = lastStrLen;
                    strCursor += lastStrLen; // keep this keyword's bytes for flush; the next appends after
                    slotState[col] = SLOT_VALUE;
                    return true;
                }
                case DATETIME -> {
                    try {
                        String s = new String(strScratch, strCursor, lastStrLen, StandardCharsets.UTF_8);
                        longSlot[col] = NdJsonSchemaInferrer.DATE_FORMATTER.parseMillis(s);
                        slotState[col] = SLOT_VALUE;
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
                boolSlot[col] = true;
                slotState[col] = SLOT_VALUE;
                return true;
            }
            if (c == 'f' && regionEquals(b, to, "false")) {
                pos += 5;
                boolSlot[col] = false;
                slotState[col] = SLOT_VALUE;
                return true;
            }
            return false;
        }
        // number. Validate the token is a CANONICAL JSON number before accepting; defer to Jackson on
        // anything non-canonical (leading zeros, leading '+', lone '-', trailing junk, ".5", "1.",
        // bad exponent), since Jackson rejects those and the fast path must never accept what Jackson
        // would reject. classify: 0 = invalid, 1 = integer, 2 = decimal.
        int numStart = pos;
        scanNumber(b, to);
        int numEnd = pos;
        int kind = classifyJsonNumber(b, numStart, numEnd);
        if (kind == 0) {
            return false;
        }
        switch (type) {
            case INTEGER -> {
                if (kind != 1) {
                    return false; // a fractional/exponent token is not an integer column value
                }
                long v = parseLongChecked(b, numStart, numEnd);
                if (numberOverflow || v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                    return false; // out of int range -> let Jackson's coercion rules decide
                }
                intSlot[col] = (int) v;
                slotState[col] = SLOT_VALUE;
                return true;
            }
            case LONG -> {
                if (kind != 1) {
                    return false;
                }
                long v = parseLongChecked(b, numStart, numEnd);
                if (numberOverflow) {
                    return false; // out of long range -> defer to Jackson
                }
                longSlot[col] = v;
                slotState[col] = SLOT_VALUE;
                return true;
            }
            case DOUBLE -> {
                // Token is canonical JSON (kind 1 or 2); the JDK parser then matches Jackson's value.
                try {
                    dblSlot[col] = Double.parseDouble(new String(b, numStart, numEnd - numStart, StandardCharsets.US_ASCII));
                    slotState[col] = SLOT_VALUE;
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
            default -> {
                return false; // number for a string/boolean column -> coercion, punt
            }
        }
    }

    /**
     * Commit the per-line slots to the page builders. Called only after a fully-decoded line, so it
     * never appends a partial row. Sets {@code touched} for every column it writes; columns left
     * {@link #SLOT_UNSET} (absent from the line) are filled by the caller's end-of-line appendNull.
     */
    private void flush(Block.Builder[] builders, java.util.BitSet touched) {
        for (int c = 0; c < types.length; c++) {
            byte st = slotState[c];
            if (st == SLOT_UNSET) {
                continue;
            }
            touched.set(c);
            if (st == SLOT_NULL) {
                builders[c].appendNull();
                continue;
            }
            switch (types[c]) {
                case LONG, DATETIME -> ((LongBlock.Builder) builders[c]).appendLong(longSlot[c]);
                case INTEGER -> ((IntBlock.Builder) builders[c]).appendInt(intSlot[c]);
                case DOUBLE -> ((DoubleBlock.Builder) builders[c]).appendDouble(dblSlot[c]);
                case BOOLEAN -> ((BooleanBlock.Builder) builders[c]).appendBoolean(boolSlot[c]);
                case KEYWORD -> {
                    scratchRef.bytes = strScratch;
                    scratchRef.offset = strOff[c];
                    scratchRef.length = strLen[c];
                    ((BytesRefBlock.Builder) builders[c]).appendBytesRef(scratchRef);
                }
                default -> throw new IllegalStateException("unsupported positional slot type: " + types[c]);
            }
        }
    }

    /**
     * Classify {@code b[s, e)} against the strict JSON number grammar
     * {@code -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?}: returns 0 (not a canonical JSON number),
     * 1 (integer), or 2 (decimal). Rejects leading zeros, a leading {@code +}, a lone {@code -},
     * {@code .5}, {@code 1.}, bad/empty exponents, and any trailing bytes — all of which Jackson rejects.
     */
    private static int classifyJsonNumber(byte[] b, int s, int e) {
        int i = s;
        if (i < e && b[i] == '-') {
            i++;
        }
        if (i >= e) {
            return 0;
        }
        if (b[i] == '0') {
            i++; // a leading zero must stand alone (no "00", "07")
        } else if (b[i] >= '1' && b[i] <= '9') {
            while (i < e && b[i] >= '0' && b[i] <= '9') {
                i++;
            }
        } else {
            return 0;
        }
        boolean decimal = false;
        if (i < e && b[i] == '.') {
            decimal = true;
            i++;
            int d = i;
            while (i < e && b[i] >= '0' && b[i] <= '9') {
                i++;
            }
            if (i == d) {
                return 0; // '.' with no following digit
            }
        }
        if (i < e && (b[i] == 'e' || b[i] == 'E')) {
            decimal = true;
            i++;
            if (i < e && (b[i] == '+' || b[i] == '-')) {
                i++;
            }
            int d = i;
            while (i < e && b[i] >= '0' && b[i] <= '9') {
                i++;
            }
            if (i == d) {
                return 0; // exponent with no digits
            }
        }
        if (i != e) {
            return 0; // trailing bytes after a complete number
        }
        return decimal ? 2 : 1;
    }

    /**
     * Parse a canonical-integer token (validated by {@link #classifyJsonNumber}) with overflow
     * detection. Accumulates in negative space so {@code Long.MIN_VALUE} parses exactly; on any
     * over/underflow sets {@link #numberOverflow} (the caller then defers to Jackson).
     */
    private long parseLongChecked(byte[] b, int from, int to) {
        numberOverflow = false;
        int i = from;
        boolean neg = b[i] == '-';
        if (neg) {
            i++;
        }
        long v = 0;
        try {
            while (i < to) {
                v = Math.subtractExact(Math.multiplyExact(v, 10), b[i] - '0');
                i++;
            }
            return neg ? v : Math.negateExact(v);
        } catch (ArithmeticException overflow) {
            numberOverflow = true;
            return 0;
        }
    }

    // ---- scanning helpers (operate on the instance cursor `pos`) ----

    private int lastStrLen;
    private boolean numberOverflow; // set by parseLongChecked when an integer token exceeds long range

    /**
     * Decode the JSON string starting at {@link #pos} (the opening quote) into {@link #strScratch},
     * setting {@link #lastStrLen}, and advance {@code pos} past the closing quote.
     *
     * @return the index just after the closing quote, or {@code -1} if malformed.
     */
    private int scanStringEnd(byte[] b, int to) {
        int i = pos + 1;       // past opening quote
        int out = strCursor;   // decode into strScratch starting at the per-line cursor (not 0)
        byte[] scratch = strScratch;
        while (i < to) {
            byte ch = b[i];
            if (ch == '"') {
                lastStrLen = out - strCursor;
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
                        if (u >= HIGH_SURROGATE_MIN && u <= HIGH_SURROGATE_MAX) { // expect a low-surrogate escape next
                            if (i + 6 > to || b[i] != '\\' || b[i + 1] != 'u') {
                                return -1;
                            }
                            int lo = hex4(b, i + 2);
                            if (lo < LOW_SURROGATE_MIN || lo > LOW_SURROGATE_MAX) {
                                return -1;
                            }
                            i += 6;
                            cp = SUPPLEMENTARY_BASE + ((u - HIGH_SURROGATE_MIN) << 10) + (lo - LOW_SURROGATE_MIN);
                        } else if (u >= LOW_SURROGATE_MIN && u <= LOW_SURROGATE_MAX) {
                            return -1; // lone low surrogate -> invalid, defer to Jackson
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
            // Raw (unescaped) byte. JSON forbids unescaped control characters, and Jackson validates
            // UTF-8 — so the fast path must too, or it would copy bytes Jackson rejects. Determine the
            // UTF-8 sequence length from the lead byte, validate it, and copy it verbatim; defer on
            // anything not well-formed (accepts valid multi-byte text such as Cyrillic, rejects junk).
            int c = ch & 0xFF;
            if (c < CONTROL_CHAR_LIMIT) {
                return -1; // unescaped control character
            }
            int seqLen;
            if (c < ASCII_LIMIT) {
                seqLen = 1;
            } else if (c >= UTF8_LEAD_2B_MIN && c <= UTF8_LEAD_2B_MAX) {
                seqLen = 2;
            } else if (c >= UTF8_LEAD_3B_MIN && c <= UTF8_LEAD_3B_MAX) {
                seqLen = 3;
            } else if (c >= UTF8_LEAD_4B_MIN && c <= UTF8_LEAD_4B_MAX) {
                seqLen = 4;
            } else {
                return -1; // invalid lead byte (0x80-0xC1 continuation/overlong, 0xF5-0xFF out of range)
            }
            if (i + seqLen > to || (seqLen > 1 && validUtf8Tail(b, i, seqLen) == false)) {
                return -1;
            }
            scratch = ensure(scratch, out + seqLen);
            for (int k = 0; k < seqLen; k++) {
                scratch[out++] = b[i + k];
            }
            i += seqLen;
        }
        return -1; // unterminated
    }

    /** Validate the continuation bytes of a multi-byte UTF-8 sequence (Unicode well-formed byte ranges). */
    private static boolean validUtf8Tail(byte[] b, int i, int seqLen) {
        int b0 = b[i] & 0xFF;
        int b1 = b[i + 1] & 0xFF;
        if (seqLen == 2) {
            return isCont(b1);
        }
        if (seqLen == 3) {
            if (b0 == UTF8_LEAD_E0 ? (b1 < UTF8_E0_MIN_2ND || b1 > UTF8_CONT_MAX)
                : b0 == UTF8_LEAD_ED ? (b1 < UTF8_CONT_MIN || b1 > UTF8_ED_MAX_2ND)
                : isCont(b1) == false) {
                return false;
            }
            return isCont(b[i + 2] & 0xFF);
        }
        // seqLen == 4
        if (b0 == UTF8_LEAD_F0 ? (b1 < UTF8_F0_MIN_2ND || b1 > UTF8_CONT_MAX)
            : b0 == UTF8_LEAD_F4 ? (b1 < UTF8_CONT_MIN || b1 > UTF8_F4_MAX_2ND)
            : isCont(b1) == false) {
            return false;
        }
        return isCont(b[i + 2] & 0xFF) && isCont(b[i + 3] & 0xFF);
    }

    private static boolean isCont(int v) {
        return v >= UTF8_CONT_MIN && v <= UTF8_CONT_MAX;
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
