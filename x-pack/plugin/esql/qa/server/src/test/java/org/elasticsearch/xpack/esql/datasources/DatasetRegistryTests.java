/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for the reserved-{@code mappings} split in {@link DatasetRegistry}: a {@code dataset:} directive's
 * {@code WITH {...}} JSON carries format settings plus, under the reserved key, a declared schema that
 * {@code PUT /_query/dataset/<name>} takes top-level rather than inside {@code settings}. Lives in the same
 * qa/server project as the class under test so it can be a plain {@link ESTestCase} unit test (no cluster,
 * no client), alongside {@link FixtureUtilsTests}.
 */
public class DatasetRegistryTests extends ESTestCase {

    /**
     * The load-bearing regression pin: a directive that declares no schema must produce the exact bytes the
     * registry produced before the split existed, so every spec file on the existing surface is untouched.
     */
    public void testBodyWithoutMappingsIsUnchanged() throws IOException {
        assertEquals(
            "{\"data_source\":\"ds\",\"resource\":\"s3://b/k\"}",
            DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), null)
        );
        assertEquals(
            "{\"data_source\":\"ds\",\"resource\":\"s3://b/k\",\"settings\":{\"header_row\":true}}",
            DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of("header_row", true), null)
        );
    }

    /** A declared schema lands top-level in the body, beside settings rather than inside them. */
    public void testDeclaredSchemaIsASiblingOfSettings() throws IOException {
        String withJson = """
            {"header_row": true, "mappings": {"dynamic": "false", "properties": {"id": {"type": "long", "path": "emp_no"}}}}""";

        DatasetRegistry.DatasetOptions options = DatasetRegistry.parseDirectiveOptions(withJson);
        assertEquals(Map.of("header_row", true), options.settings());
        assertEquals(Map.of("dynamic", "false", "properties", Map.of("id", Map.of("type", "long", "path", "emp_no"))), options.mappings());

        Map<String, Object> body = parseJson(DatasetRegistry.datasetRequestBody("ds", "s3://b/k", options.settings(), options.mappings()));
        assertEquals(Map.of("header_row", true), body.get("settings"));
        assertEquals(options.mappings(), body.get("mappings"));
    }

    /**
     * A directive that declares only a schema leaves the settings map empty, so the body omits {@code settings}
     * entirely — the omit-when-empty behaviour the surface already relies on.
     */
    public void testMappingsOnlyDirectiveOmitsSettings() throws IOException {
        DatasetRegistry.DatasetOptions options = DatasetRegistry.parseDirectiveOptions("{\"mappings\": {\"dynamic\": \"false\"}}");
        assertEquals(Map.of(), options.settings());

        String body = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", options.settings(), options.mappings());
        assertEquals("{\"data_source\":\"ds\",\"resource\":\"s3://b/k\",\"mappings\":{\"dynamic\":\"false\"}}", body);
    }

    /** A null directive declares nothing: no settings, no schema. */
    public void testNoWithClauseDeclaresNothing() throws IOException {
        DatasetRegistry.DatasetOptions options = DatasetRegistry.parseDirectiveOptions(null);
        assertEquals(Map.of(), options.settings());
        assertNull(options.mappings());
    }

    /**
     * A reserved key whose value is not an object is a spec-authoring error, so it fails where the directive
     * text is still at hand rather than as a type error from the server or a {@link ClassCastException} here.
     */
    public void testNonObjectMappingsValueIsRejected() {
        for (String value : new String[] { "\"strict\"", "3", "[{\"dynamic\": \"false\"}]", "null" }) {
            String withJson = "{\"mappings\": " + value + "}";
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DatasetRegistry.parseDirectiveOptions(withJson)
            );
            assertThat(e.getMessage(), containsString("[mappings] in a dataset directive's WITH must be a JSON object"));
        }
    }

    /**
     * The harness guards the paths that cannot carry a declaration off this predicate, so it must key on the
     * reserved name as a KEY: a setting whose value merely equals {@code "mappings"} is not a declaration.
     */
    public void testDeclaresMappingsKeysOnTheNameNotTheText() {
        assertTrue(DatasetRegistry.declaresMappings("{\"mappings\": {\"dynamic\": \"false\"}}"));
        assertTrue(DatasetRegistry.declaresMappings("{\"header_row\": true, \"mappings\": {\"dynamic\": \"true\"}}"));

        assertFalse(DatasetRegistry.declaresMappings(null));
        assertFalse(DatasetRegistry.declaresMappings("{}"));
        assertFalse(DatasetRegistry.declaresMappings("{\"header_row\": true}"));
        assertFalse(DatasetRegistry.declaresMappings("{\"column_prefix\": \"mappings\"}"));
    }

    /**
     * Nesting is why the reserved key needs no grammar change, and why the settings map must come back
     * without it: a multi-column declaration is several objects deep.
     */
    public void testNestedDeclarationRoundTripsIntoTheBody() throws IOException {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("id", Map.of("type", "long", "path", "emp_no"));
        properties.put("hired", Map.of("type", "date", "path", "hire_date", "format", "yyyy-MM-dd"));

        String withJson = """
            {"mappings": {"dynamic": "false", "_id": {"path": "emp_no"}, "properties": {\
            "id": {"type": "long", "path": "emp_no"}, \
            "hired": {"type": "date", "path": "hire_date", "format": "yyyy-MM-dd"}}}}""";

        DatasetRegistry.DatasetOptions options = DatasetRegistry.parseDirectiveOptions(withJson);
        assertEquals(Map.of(), options.settings());
        assertEquals(Map.of("path", "emp_no"), options.mappings().get("_id"));
        assertEquals(properties, options.mappings().get("properties"));

        Map<String, Object> body = parseJson(DatasetRegistry.datasetRequestBody("ds", "s3://b/k", options.settings(), options.mappings()));
        assertFalse("settings must be omitted when the directive declares only a schema", body.containsKey("settings"));
        assertEquals(options.mappings(), body.get("mappings"));
    }

    /**
     * The order of {@code properties} is the declared schema's column order, which a declared read surfaces as its
     * output column order -- so the body must carry the properties in the order the directive wrote them. Parsing
     * into an unordered map would return the columns in an arbitrary order. (Binding itself is BY NAME, keyed on the
     * schema's provenance rather than on whether a {@code path} was written -- see
     * {@code FormatReader#withDeclaredPathBinding} -- so this is about column order, not about which column is read.)
     */
    public void testDeclarationKeepsPropertyOrder() throws IOException {
        // Names chosen so insertion order is neither alphabetical nor hash order, i.e. an unordered parse
        // reliably scrambles them rather than coincidentally agreeing.
        String withJson = "{\"mappings\": {\"dynamic\": \"false\", \"properties\": {"
            + "\"zulu\": {\"type\": \"keyword\"}, "
            + "\"alpha\": {\"type\": \"long\"}, "
            + "\"mike\": {\"type\": \"keyword\"}, "
            + "\"bravo\": {\"type\": \"long\"}}}}";

        DatasetRegistry.DatasetOptions options = DatasetRegistry.parseDirectiveOptions(withJson);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) options.mappings().get("properties");
        assertEquals(List.of("zulu", "alpha", "mike", "bravo"), List.copyOf(properties.keySet()));

        // The serialized body is what the server actually binds against, so pin the order there too -- an ordered
        // parse that a re-ordering builder undid would still bind the wrong columns.
        String body = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", options.settings(), options.mappings());
        assertThat(
            body,
            containsString(
                "\"properties\":{\"zulu\":{\"type\":\"keyword\"},\"alpha\":{\"type\":\"long\"},"
                    + "\"mike\":{\"type\":\"keyword\"},\"bravo\":{\"type\":\"long\"}}"
            )
        );
    }

    private static Map<String, Object> parseJson(String json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }
}
