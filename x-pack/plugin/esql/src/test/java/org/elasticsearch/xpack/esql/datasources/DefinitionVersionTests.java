/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The version exists so that everything derived from a dataset's definitions is addressed by those
 * definitions. These cases pin both directions: an edit must change it, and an unchanged definition
 * must not. Only the second keeps the mechanism from being equivalent to disabling the cache.
 */
public class DefinitionVersionTests extends ESTestCase {

    private static DataSource source(Map<String, Object> settings) {
        Map<String, DataSourceSetting> wrapped = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : settings.entrySet()) {
            boolean secret = e.getKey().contains("key") || e.getKey().contains("token");
            wrapped.put(e.getKey(), new DataSourceSetting(e.getValue(), secret));
        }
        return new DataSource("src", "s3", null, wrapped);
    }

    private static Dataset dataset(String resource, Map<String, Object> settings) {
        return new Dataset("parts", new DataSourceReference("src"), resource, null, settings);
    }

    public void testSameDefinitionsProduceTheSameVersion() {
        Map<String, Object> dsSettings = Map.of("format", "csv", "error_mode", "null_field");
        Map<String, Object> srcSettings = Map.of("endpoint", "https://s3.example", "access_key", "AAA");

        String a = DefinitionVersion.of(dataset("s3://b/*.csv", dsSettings), source(srcSettings));
        String b = DefinitionVersion.of(dataset("s3://b/*.csv", dsSettings), source(srcSettings));
        assertEquals("an unchanged definition keeps its version, or nothing is ever warm", a, b);
    }

    public void testSettingOrderDoesNotChangeTheVersion() {
        Map<String, Object> ordered = new LinkedHashMap<>();
        ordered.put("format", "csv");
        ordered.put("error_mode", "null_field");
        Map<String, Object> reversed = new LinkedHashMap<>();
        reversed.put("error_mode", "null_field");
        reversed.put("format", "csv");

        assertEquals(
            "the version is of the definition, not of the order it happened to be stored in",
            DefinitionVersion.of(dataset("s3://b/*.csv", ordered), source(Map.of("endpoint", "e"))),
            DefinitionVersion.of(dataset("s3://b/*.csv", reversed), source(Map.of("endpoint", "e")))
        );
    }

    public void testEditingADatasetSettingChangesTheVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        String before = DefinitionVersion.of(dataset("s3://b/*.csv", Map.of("format", "csv", "error_mode", "fail_fast")), src);
        String after = DefinitionVersion.of(dataset("s3://b/*.csv", Map.of("format", "csv", "error_mode", "null_field")), src);
        assertNotEquals("an edited setting must take what was derived under the old one out of reach", before, after);
    }

    public void testEditingTheResourceChangesTheVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        Map<String, Object> settings = Map.of("format", "csv");
        assertNotEquals(
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), src),
            DefinitionVersion.of(dataset("s3://b/other/*.csv", settings), src)
        );
    }

    /**
     * The case this mechanism exists for. Credentials are absent from every other component of a cache
     * key, so before this nothing about a rotation reached the key and entries harvested under the old
     * credentials stayed addressable.
     */
    public void testRotatingACredentialOnTheDataSourceChangesTheVersion() {
        Map<String, Object> settings = Map.of("format", "csv");
        String before = DefinitionVersion.of(
            dataset("s3://b/*.csv", settings),
            source(Map.of("endpoint", "https://s3.example", "access_key", "AAA", "secret_key", "BBB"))
        );
        String after = DefinitionVersion.of(
            dataset("s3://b/*.csv", settings),
            source(Map.of("endpoint", "https://s3.example", "access_key", "CCC", "secret_key", "DDD"))
        );
        assertNotEquals("a credential rotation must take what was derived under the old one out of reach", before, after);
    }

    /** A dataset inherits its data source's settings, so an edit to the source reaches the dataset's version. */
    public void testEditingTheDataSourceEndpointChangesTheVersion() {
        Map<String, Object> settings = Map.of("format", "csv");
        assertNotEquals(
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), source(Map.of("endpoint", "https://s3.example"))),
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), source(Map.of("endpoint", "https://other.example")))
        );
    }

    /** No credential value may appear in what the version is, since the version reaches a cache key. */
    public void testTheVersionCarriesNoCredentialValue() {
        String version = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            source(Map.of("endpoint", "https://s3.example", "access_key", "AKIAEXAMPLESECRET", "secret_key", "sh4redS3cret"))
        );
        assertFalse("a credential must not survive into the version", version.contains("AKIAEXAMPLESECRET"));
        assertFalse("a credential must not survive into the version", version.contains("sh4redS3cret"));
    }
}
