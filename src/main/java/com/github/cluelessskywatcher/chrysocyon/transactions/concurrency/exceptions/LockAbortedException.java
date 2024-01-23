package com.github.cluelessskywatcher.chrysocyon.transactions.concurrency.exceptions;

public class LockAbortedException extends RuntimeException {
    public LockAbortedException(String message) {
        super(message);
    }
}
