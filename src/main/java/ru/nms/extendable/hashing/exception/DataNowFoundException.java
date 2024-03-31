package ru.nms.extendable.hashing.exception;

public class DataNowFoundException extends RuntimeException{
    public DataNowFoundException(long id){
        super("Data with id " + id + " was not found");
    }
}
