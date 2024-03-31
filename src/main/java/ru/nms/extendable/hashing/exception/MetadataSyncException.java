package ru.nms.extendable.hashing.exception;

public class MetadataSyncException extends RuntimeException{
    public MetadataSyncException() {
        super("Data id does not match metadata id");
    }
}
