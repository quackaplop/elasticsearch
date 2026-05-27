/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.esql.datasources;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonSchemaInferrer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Real-execution-path benchmark for the schema-positional NDJSON decoder (elastic/esql-planning#710):
 * drives {@link NdJsonFormatReader#read} — the actual reader + {@code NdJsonPageDecoder} — over an
 * in-memory NDJSON buffer, toggling the {@code esql.datasource.ndjson.positional_decoding} setting
 * (not a test hook). Reports rows/sec so positional (setting on) can be compared to Jackson (off).
 *
 * <p>The {@code multiValue} axis emits array values on one column: positional defers any line with an
 * array to Jackson, so {@code multiValue=true} measures the fallback-heavy case (positional ~ Jackson),
 * while {@code multiValue=false} measures the all-scalar fast path (where positional is expected to win).
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class NdJsonPositionalDecodeBenchmark {

    static final int ROWS = 16_384;
    static final int BATCH = 1_024;

    @Param({ "false", "true" })
    boolean positional;

    @Param({ "false", "true" })
    boolean multiValue;

    private static final String[] NAMES = {
        "WatchID",
        "JavaEnable",
        "Title",
        "GoodEvent",
        "EventTime",
        "CounterID",
        "ClientIP",
        "RegionID",
        "UserID",
        "URL",
        "IsRefresh",
        "ResolutionWidth" };
    private static final DataType[] TYPES = {
        DataType.LONG,
        DataType.INTEGER,
        DataType.KEYWORD,
        DataType.INTEGER,
        DataType.LONG,
        DataType.INTEGER,
        DataType.INTEGER,
        DataType.INTEGER,
        DataType.LONG,
        DataType.KEYWORD,
        DataType.INTEGER,
        DataType.INTEGER };

    private byte[] data;
    private List<Attribute> schema;
    private NdJsonFormatReader reader;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        BlockFactory bf = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        schema = new ArrayList<>(NAMES.length);
        for (int c = 0; c < NAMES.length; c++) {
            schema.add(NdJsonSchemaInferrer.attribute(NAMES[c], TYPES[c], true));
        }
        Settings settings = Settings.builder().put("esql.datasource.ndjson.positional_decoding", positional).build();
        reader = new NdJsonFormatReader(settings, bf, schema);
        data = generate(ROWS, multiValue, 42L);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long decode() throws IOException {
        long rows = 0;
        StorageObject object = new BytesObject("bench://ndjson", data);
        try (CloseableIterator<Page> it = reader.read(object, null, BATCH)) {
            while (it.hasNext()) {
                Page p = it.next();
                rows += p.getPositionCount();
                p.releaseBlocks();
            }
        }
        return rows;
    }

    private static byte[] generate(int rows, boolean multiValue, long seed) {
        Random rnd = new Random(seed);
        StringBuilder sb = new StringBuilder(rows * 256);
        for (int r = 0; r < rows; r++) {
            sb.append('{');
            for (int c = 0; c < NAMES.length; c++) {
                if (c > 0) {
                    sb.append(',');
                }
                sb.append('"').append(NAMES[c]).append("\":");
                // Emit an array on one INT column when multiValue is set (positional defers -> Jackson).
                if (multiValue && c == NAMES.length - 1) {
                    sb.append('[').append(rnd.nextInt(10_000)).append(',').append(rnd.nextInt(10_000)).append(']');
                    continue;
                }
                switch (TYPES[c]) {
                    case LONG -> sb.append(rnd.nextLong() >>> 1);
                    case INTEGER -> sb.append(rnd.nextInt(1_000_000));
                    case KEYWORD -> {
                        sb.append('"');
                        int len = 8 + rnd.nextInt(40);
                        for (int i = 0; i < len; i++) {
                            sb.append((char) ('a' + rnd.nextInt(26)));
                        }
                        sb.append('"');
                    }
                    default -> throw new AssertionError();
                }
            }
            sb.append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Minimal in-memory {@link StorageObject} with a known length so the reader takes the byte-array path. */
    private record BytesObject(String location, byte[] bytes) implements StorageObject {
        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(bytes, (int) position, (int) length);
        }

        @Override
        public long length() {
            return bytes.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of(location);
        }
    }
}
