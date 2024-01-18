package com.github.cluelessskywatcher.chrysocyon.metrics.impl.filesystem;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.metrics.AbstractMetric;

public class BlocksReadMetric extends AbstractMetric {
    @Override
    public Object getMetricStats() {
        return Chrysocyon.getInstance().getFileManager().getBlocksRead();
    }    
}
