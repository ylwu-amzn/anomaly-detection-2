package org.opensearch.ad.task;

public class ADTaskSlotLimit {

    private Integer detectorTaskSlots;
    private Integer detectorTaskLaneLimit;

    public ADTaskSlotLimit(Integer detectorTaskSlots, Integer detectorTaskLaneLimit) {
        this.detectorTaskSlots = detectorTaskSlots;
        this.detectorTaskLaneLimit = detectorTaskLaneLimit;
    }

    public Integer getDetectorTaskSlots() {
        return detectorTaskSlots;
    }

    public void setDetectorTaskSlots(Integer detectorTaskSlots) {
        this.detectorTaskSlots = detectorTaskSlots;
    }

    public Integer getDetectorTaskLaneLimit() {
        return detectorTaskLaneLimit;
    }

    public void setDetectorTaskLaneLimit(Integer detectorTaskLaneLimit) {
        this.detectorTaskLaneLimit = detectorTaskLaneLimit;
    }
}
