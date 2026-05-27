/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The same NDJSON csv-spec correctness suite as {@link NdJsonFormatSpecIT}, but with the
 * schema-positional decode fast path turned ON ({@code esql.datasource.ndjson.positional_decoding}).
 * Every query + expected-table spec must produce the same results through the positional path as it
 * does through Jackson (the default IT) — end-to-end proof, through the real query engine and real
 * storage backends (S3 / HTTP / LOCAL), that enabling the setting changes throughput only, not
 * results. Complements the unit + page-level differential tests with full-stack coverage.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class NdJsonPositionalFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        () -> s3Fixture.getAddress(),
        config -> config.setting("esql.datasource.ndjson.positional_decoding", "true")
    );

    /** Same STRICT multi-file mutes as {@link NdJsonFormatSpecIT} (pre-existing fixture/STRICT gap). */
    private static final Set<String> SKIPPED_TESTS = Set.of("strictCount", "strictFilterAndSort", "strictSalaryStats", "strictAggregateByGender");

    public NdJsonPositionalFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "ndjson");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " not supported by NDJSON multi-file path (SchemaAdaptingIterator limitation)", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/external-basic.csv-spec", "/external-multifile.csv-spec", "/external-multifile-resolution.csv-spec");
    }
}
