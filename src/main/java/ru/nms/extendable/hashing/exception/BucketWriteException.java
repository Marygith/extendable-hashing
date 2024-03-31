package ru.nms.extendable.hashing.exception;

public class BucketWriteException extends RuntimeException{
    public BucketWriteException(String meta, int dataLength, int localDepth) {
        super("Failed writing to bucket " + meta + "data with length " + dataLength + " and local depth " + localDepth);
    }
}
