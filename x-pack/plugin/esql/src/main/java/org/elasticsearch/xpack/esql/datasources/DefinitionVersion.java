/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/**
 * A version of the stored definitions a query reads a dataset under: the dataset's own definition and
 * that of the data source it references, folded into one opaque value.
 * <p>
 * Everything cached about a file is derived from those definitions, so everything cached about it is
 * addressed by this. An edit to either — a setting, the resource pattern, an endpoint, a credential —
 * yields a different version, so entries derived under the old one are no longer reachable and age out.
 * That replaces an invalidation path from the registry to the caches: nothing has to notice a change
 * and tell anyone about it, because the address moved.
 * <p>
 * Deliberately coarse. An edit that could not have changed what is cached still changes the version,
 * and the cost is one cold read. Deciding per field which edits matter would have to be re-decided
 * for every setting added, and being wrong that way is silent — an entry keeps being served after the
 * thing it was derived from has changed.
 * <p>
 * Credential values are folded in, never carried: they reach only {@link MurmurHash3}, and what comes
 * out is a digest. A rotation changes the version without a secret entering a cache key.
 */
public final class DefinitionVersion {

    /**
     * Key under which the version travels in a query's merged config map, alongside the settings it is
     * computed from. Chosen to collide with no setting name a user can register.
     */
    public static final String CONFIG_KEY = "_definition_version";

    private DefinitionVersion() {}

    /**
     * The version for {@code dataset} read under {@code parent}. Both are folded in, because a dataset
     * inherits its data source's settings: rotating a credential on the source changes what every
     * dataset over it reads, and must change their versions too.
     */
    public static String of(Dataset dataset, DataSource parent) {
        StringBuilder encoded = new StringBuilder();
        encoded.append("ds:").append(dataset.name()).append('\u0000');
        encoded.append("res:").append(dataset.resource()).append('\u0000');
        encodeSettings(encoded, dataset.settings());
        // The declared mapping decides which columns are read and at what types, so two datasets over
        // one resource that differ only in their mapping must not share a version.
        encoded.append("map:").append(dataset.mapping()).append('\u0000');

        encoded.append("src:").append(parent.name()).append('\u0000');
        encoded.append("type:").append(parent.type()).append('\u0000');
        encodeDataSourceSettings(encoded, parent);

        byte[] bytes = encoded.toString().getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
        return Long.toHexString(hash.h1) + Long.toHexString(hash.h2);
    }

    /** Sorted, so two equal definitions encode identically whatever order their settings were stored in. */
    private static void encodeSettings(StringBuilder encoded, Map<String, Object> settings) {
        for (Map.Entry<String, Object> e : new TreeMap<>(settings).entrySet()) {
            encoded.append(e.getKey()).append('=').append(e.getValue()).append('\u0000');
        }
    }

    /**
     * A data source's settings, secrets included. {@link DataSourceSetting#rawValue()} is the encrypted
     * carrier for a secret rather than its plaintext, and either way only its digest survives this
     * method — what matters is that the value changes when the credential does.
     */
    private static void encodeDataSourceSettings(StringBuilder encoded, DataSource parent) {
        Map<String, Object> sorted = new TreeMap<>();
        for (Map.Entry<String, DataSourceSetting> e : parent.settings()) {
            sorted.put(e.getKey(), e.getValue().rawValue());
        }
        for (Map.Entry<String, Object> e : sorted.entrySet()) {
            encoded.append(e.getKey()).append('=').append(e.getValue()).append('\u0000');
        }
    }
}
