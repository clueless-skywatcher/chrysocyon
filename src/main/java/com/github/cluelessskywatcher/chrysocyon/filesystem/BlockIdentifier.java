package com.github.cluelessskywatcher.chrysocyon.filesystem;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class BlockIdentifier {
    private @Getter String fileName;
    private @Getter int blockNumber;

    public boolean equals(Object other) {
        if (!(other instanceof BlockIdentifier)) return false;

        BlockIdentifier otherIdentifier = (BlockIdentifier) other;

        return this.fileName.equals(otherIdentifier.fileName)
            && this.blockNumber == otherIdentifier.blockNumber;
    }

    public String toString() {
        return String.format("(File: %s, Block: %d)", fileName, blockNumber);
    }

    public int hashCode() {
        return toString().hashCode();
    }
}
