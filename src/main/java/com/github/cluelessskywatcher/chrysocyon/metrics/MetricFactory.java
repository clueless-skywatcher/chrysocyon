package com.github.cluelessskywatcher.chrysocyon.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.metrics.impl.filesystem.BlocksReadMetric;
import com.github.cluelessskywatcher.chrysocyon.metrics.impl.filesystem.BlocksWrittenMetric;

public class MetricFactory {
    private static final Map<MetricEnum, AbstractMetric> METRICS;

    static {
        Map<MetricEnum, AbstractMetric> map = new HashMap<>();

        map.put(MetricEnum.BLOCKS_READ, new BlocksReadMetric());
        map.put(MetricEnum.BLOCKS_WRITTEN, new BlocksWrittenMetric());

        METRICS = Collections.unmodifiableMap(map);
    }

    public AbstractMetric getMetric(MetricEnum metric) {
        return METRICS.get(metric);
    }

    public AbstractMetric getMetric(String metricName) {
        return METRICS.get(MetricEnum.getFromName(metricName));
    }

    public Object getMetricStats(String metricName) {
        return getMetric(metricName).getMetricStats();
    }
}
