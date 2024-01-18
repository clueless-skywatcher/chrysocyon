package com.github.cluelessskywatcher.chrysocyon.metrics;

import java.util.HashMap;
import java.util.Map;

public enum MetricEnum {
    BLOCKS_READ("filesystem.blocksread"),
    BLOCKS_WRITTEN("filesystem.blockswritten");

    private final String metricName;
    private static final Map<String, MetricEnum> lookup = new HashMap<String, MetricEnum>();

    static {
        for (MetricEnum m : MetricEnum.values()) {
            lookup.put(m.getName(), m);
        }
    }

    private MetricEnum(String metricName) {
        this.metricName = metricName;
    }

    public String getName() {
        return this.metricName;
    }

    public static MetricEnum getFromName(String name) {
        return lookup.get(name);
    }
}
