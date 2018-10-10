package peku.gmtt;

public class GMTTStateTransition {
    private Long startTime;
    private Long endTime;

    public GMTTStateTransition(Long startTime, Long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }
}
