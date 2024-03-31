package ru.nms.extendable.hashing.exception;

public class DataWriteException extends RuntimeException{
    public DataWriteException() {
        super("Didn't manage to put new data to the storage");
    }
}
