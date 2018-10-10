package peku.gmtt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

// Class that captures transition times for one specific transition
public class GMTTStateTransitionStatistics {
    private Long histogramBucket = Long.valueOf(60000);
    boolean statisticsUpToDate = false;

    Double mean = null;
    Long median = null;
    Double skew = null;
    Double stdev = null;

    Long firstBucket = Long.MAX_VALUE;
    Long lastBucket = Long.MIN_VALUE;

    // duration + completion time
    // private ArrayList<GMTTSTateTransition> transitions;
    private Map<Long, ArrayList<GMTTStateTransition>> histogramTransitionTimes = new HashMap<>();

    // For each bucket,calculate the mean, stdev, etc.
    // And the same across all buckets
    public void CalculateGMTTStateTransitionStatistics() {
        // TODO: implement calculation of statistics

        statisticsUpToDate = true;
    }

    public GMTTStateTransitionStatistics(Long bucketSize) {
        histogramBucket = Long.valueOf(bucketSize);
    }

    public void AddTransition (GMTTStateTransition transition) {
        statisticsUpToDate = false;
        Long index = transition.getEndTime() / histogramBucket;

        if (!histogramTransitionTimes.containsKey(index)) {
            ArrayList<GMTTStateTransition> i = new ArrayList<>();
            histogramTransitionTimes.put(index, i);
            i.add(transition);
        } else {
            ArrayList<GMTTStateTransition> i = histogramTransitionTimes.get(index);
            i.add(transition);
        }
        if (index < firstBucket) { firstBucket = index; }
        if (index > lastBucket) { lastBucket = index; }
    }

    // Provides a histogram as a map with per bucket the number of state transitions
    public Map<Long, Integer> getHistogramMap () {
        Map<Long, Integer> result = new LinkedHashMap<>();

        for (Long i = firstBucket; i < lastBucket; i++) {
            if (histogramTransitionTimes.containsKey(i)) {
                result.put(i, histogramTransitionTimes.get(i).size());
            } else {
                result.put(i, 0);
            }
        }
        return result;
    }

    public Double getMean() {
        if (!statisticsUpToDate) {
            CalculateGMTTStateTransitionStatistics();
        }
        return mean;
    }

    public Long getMedian() {
        if (!statisticsUpToDate) {
            CalculateGMTTStateTransitionStatistics();
        }
        return median;
    }

    public Double getSkew() {
        if (!statisticsUpToDate) {
            CalculateGMTTStateTransitionStatistics();
        }
        return skew;
    }

    public Double getStdev() {
        if (!statisticsUpToDate) {
            CalculateGMTTStateTransitionStatistics();
        }
        return stdev;
    }

    public Long getFirstBucket() {
        return firstBucket;
    }

    public Long getLastBucket() {
        return lastBucket;
    }
}
