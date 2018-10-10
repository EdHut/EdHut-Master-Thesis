package peku.gmtt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

// Class to build histogram for status transitions, which may for instance be case-arrivals and -departures (to another status)
public class GMTTStatusHistogram {
    private Long histogramBucket = Long.valueOf(60000);

    private Long lowestBucket = Long.MAX_VALUE;
    private Long highestBucket = Long.MIN_VALUE;
    private Map<Long, Integer> histogram = new HashMap<>();

    public GMTTStatusHistogram (Long bucketSize) {
        histogramBucket = Long.valueOf(bucketSize);
    }

    public void Add (Long time) {
        Long index = time / histogramBucket;

        if (!histogram.containsKey(index)) {
            histogram.put(index, 1);
        } else {
            histogram.replace(index, histogram.get(index) + 1);
        }
        if (lowestBucket > index) { lowestBucket = index; }
        if (highestBucket < index) { highestBucket = index; }
    }

    public Map<Long, Integer> getHistogramCount() {
        return histogram;  // Note: this is an unsafe way to return result, since any caller will get reference to the map and can manipulate it.
    }

    public Long getLowestBucket() {
        return lowestBucket;
    }

    public Long getHighestBucket() {
        return highestBucket;
    }
}
