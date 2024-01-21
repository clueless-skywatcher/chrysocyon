package com.github.cluelessskywatcher.chrysocyon.buffer.replacement;

import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;

public enum ReplacementStrategyEnum {
    NAIVE;

    public static BufferReplacementStrategy getStrategy(BufferPoolManager manager, ReplacementStrategyEnum strategy) {
        if (strategy == NAIVE) {
            return new NaiveReplacementStrategy(manager);
        }

        return null;
    }
}
