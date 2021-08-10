package org.opensearch.ad.transport;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import java.io.IOException;

public abstract class ADBaseNodesRequest extends BaseNodesRequest<ADBaseNodesRequest> {

    private String adVersion;

    public ADBaseNodesRequest(StreamInput in) throws IOException {
        super(in);
        this.adVersion = in.readString();
    }

    protected ADBaseNodesRequest(DiscoveryNode... concreteNodes) {
        super(concreteNodes);
    }

    public ADBaseNodesRequest(String adVersion, DiscoveryNode[] nodes) {
        super(nodes);
        this.adVersion = adVersion;
    }

    public String getAdVersion() {
        return this.adVersion;
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}


