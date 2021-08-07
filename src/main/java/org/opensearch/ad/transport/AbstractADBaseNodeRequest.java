package org.opensearch.ad.transport;

import org.opensearch.LegacyESVersion;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

public abstract class AbstractADBaseNodeRequest extends BaseNodeRequest {
    public AbstractADBaseNodeRequest(StreamInput in) throws IOException {
        super(in);
    }

    public AbstractADBaseNodeRequest() {

    }

    public abstract boolean isCompatible();
}
