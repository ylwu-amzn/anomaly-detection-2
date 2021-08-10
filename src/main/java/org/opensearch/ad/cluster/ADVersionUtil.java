package org.opensearch.ad.cluster;

import org.opensearch.Version;

public class ADVersionUtil {

    public static final int VERSION_SEGMENTS = 3;

    public static Version fromString(String adVersion) {
        return Version.fromString(normalizeVersion(adVersion));
    }

    public static String normalizeVersion(String adVersion) {
        if (adVersion == null) {
            throw new IllegalArgumentException("Wrong AD version " + adVersion);
        }
        String[] versions = adVersion.split("\\.");
        if (versions.length < VERSION_SEGMENTS) {
            throw new IllegalArgumentException("Wrong AD version " + adVersion);
        }
        StringBuilder normalizedVersion = new StringBuilder();
        normalizedVersion.append(versions[0]);
        for (int i = 1; i < VERSION_SEGMENTS; i++) {
            normalizedVersion.append(".");
            normalizedVersion.append(versions[i]);
        }
        return normalizedVersion.toString();
    }
}
