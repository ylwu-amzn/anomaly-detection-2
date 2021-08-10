package org.opensearch.ad.cluster;

import org.opensearch.Version;

public class ADVersionUtil {

    public static Version fromString(String adVersion) {
        return Version.fromString(adVersion.substring(0, adVersion.lastIndexOf(".")));
    }
}
