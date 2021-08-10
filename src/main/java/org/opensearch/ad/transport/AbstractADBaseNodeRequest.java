package org.opensearch.ad.transport;

import org.opensearch.LegacyESVersion;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

public class AbstractADBaseNodeRequest extends BaseNodeRequest {
    private String adVersion;

    public AbstractADBaseNodeRequest(StreamInput in) throws IOException {
        super(in);
    }

    public AbstractADBaseNodeRequest() {

    }

    public String getAdVersion() {
        return this.adVersion;
    }

    public void setAdVersion(String adVersion) {
        this.adVersion = adVersion;
    }
}
