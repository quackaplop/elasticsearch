/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.json;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

public class XContextExtractorTests extends ESTestCase {
    public static final class ByteArrayStream extends ByteArrayInputStream {

        ByteArrayStream(final byte[] bytes) {
            super(bytes);
        }

        int getPosition() {
            // pos is protected in ByteArrayInputStream
            return pos;
        }

        void unreadByte() {
            if (pos > 0) {
                pos--;
            }
        }
    }
    private static final String STRING_FIELD = "string_field";
    private static final String INT_FIELD = "int_field";
    private static final String FLOAT_FIELD = "float_field";
    private static final String BOOLEAN_FIELD = "boolean_field";
    private static final String NULL_FIELD = "null_field";
    private static final String ARRAY_FIELD = "array_field";
    private static final String OBJECT_FIELD = "object_field";

    public void testNothing() {
        try {
            byte[] payload = generatePayload(jsonXContent);
            assertNotNull(payload);
            var result = new String(payload, StandardCharsets.UTF_8);
            System.out.println(">>>" + result);
            ByteArrayStream buffer = new ByteArrayStream(payload);
            try (var parser = jsonXContent.createParser(XContentParserConfiguration.EMPTY, buffer)) {
                String value = extractValue(parser, buffer,"string_field");
                assertNotNull(value);
                System.out.println(">>>" + value);
            }
        } catch (Exception e) {
            fail("Exception should not be thrown: " + e.getMessage());
        }
    }

    /**
     * Generates a well-formed payload in a format specified through the xContent parameter.
     */
    private static byte[] generatePayload(XContent xContent) throws Exception {
        try (var outputStream = new ByteArrayOutputStream()) {
            try (var generator = xContent.createGenerator(outputStream)) {
                generator.writeStartObject();

                generator.writeStringField(STRING_FIELD, randomAlphanumericOfLength(10));
                generator.writeNumberField(INT_FIELD, randomInt(1_000));
                generator.writeNumberField(FLOAT_FIELD, randomFloatBetween(0, 1_000, true));
                generator.writeBooleanField(BOOLEAN_FIELD, randomBoolean());
                generator.writeNullField(NULL_FIELD);

                // Array of strings
                generator.writeFieldName(ARRAY_FIELD);
                generator.writeStartArray();
                for (int i = 0; i < randomIntBetween(0, 10); i++) {
                    generator.writeString(randomAlphanumericOfLength(5));
                }
                generator.writeEndArray();

                // Object field
                generator.writeFieldName(OBJECT_FIELD);
                generator.writeStartObject();
                generator.writeStringField(STRING_FIELD, randomAlphanumericOfLength(10));
                generator.writeNumberField(INT_FIELD, randomInt(1_000));
                generator.writeEndObject();

                generator.writeEndObject();
            }
            return outputStream.toByteArray();
        }
    }

    public static String extractValue(XContentParser parser, ByteArrayStream buffer, String path) throws IOException {
        var p = seekToField(parser, path);
        System.out.println(">>>" + p.text());
        System.out.println(">>>" + p.getTokenLocation());
        System.out.println(">>>" + buffer.getPosition());
        return "";
    }

    private static XContentParser seekToField(XContentParser parser, String fieldName) throws IOException {
        XContentParser.Token token = parser.currentToken();
        while ((token = parser.nextToken()) != null) {
            // Skip anything that is not a field name. We rely on the code below to drain field names and their values
            if (token == XContentParser.Token.FIELD_NAME) {
                // field name matches, get to the value token and return the parser
                if (parser.currentName().equals(fieldName)) {
                    parser.nextToken();
                    return parser;
                } else {
                    // file name didn't match, mo ve on
                    parser.nextToken();
                    parser.skipChildren();
                }
            }
        }
        return null;
    }

    private static String extractRecursive(XContentParser parser, String[] segments, int index) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != null) {
            if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals(arrayKey(segments[index]))) {
                parser.nextToken();
                String segment = segments[index];
                if (isArray(segment)) {
                    int arrIdx = arrayIndex(segment);
                    if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                        for (int i = 0; i <= arrIdx; i++) {
                            parser.nextToken();
                            if (i == arrIdx) {
                                if (index == segments.length - 1) {
                                    return readValue(parser);
                                } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                                    return extractRecursive(parser, segments, index + 1);
                                }
                            } else {
                                parser.skipChildren();
                            }
                        }
                    }
                } else if (index == segments.length - 1) {
                    return readValue(parser);
                } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    return extractRecursive(parser, segments, index + 1);
                }
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return null;
    }

    private static String arrayKey(String segment) {
        int idx = segment.indexOf('[');
        return idx == -1 ? segment : segment.substring(0, idx);
    }

    private static boolean isArray(String segment) {
        return segment.contains("[") && segment.endsWith("]");
    }

    private static int arrayIndex(String segment) {
        int start = segment.indexOf('[') + 1;
        int end = segment.indexOf(']');
        return Integer.parseInt(segment.substring(start, end));
    }

    private static String readValue(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            return parser.numberValue().toString();
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return Boolean.toString(parser.booleanValue());
        } else if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token == XContentParser.Token.START_OBJECT) {
            return parser.map().toString();
        } else if (token == XContentParser.Token.START_ARRAY) {
            return parser.list().toString();
        }
        return null;
    }
}
