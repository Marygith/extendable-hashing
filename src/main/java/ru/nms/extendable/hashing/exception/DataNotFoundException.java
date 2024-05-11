package ru.nms.extendable.hashing.exception;

public class DataNotFoundException extends RuntimeException{
    public DataNotFoundException(long id){
        super("Data with id " + id + " was not found");
    }
}
