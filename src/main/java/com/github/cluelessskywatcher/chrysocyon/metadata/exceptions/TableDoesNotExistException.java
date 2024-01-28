package com.github.cluelessskywatcher.chrysocyon.metadata.exceptions;

public class TableDoesNotExistException extends RuntimeException {
    public TableDoesNotExistException(String message) {
        super(message);
    }
}
