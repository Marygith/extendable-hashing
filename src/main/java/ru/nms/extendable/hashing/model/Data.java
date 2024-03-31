package ru.nms.extendable.hashing.model;

import lombok.ToString;

@ToString
@lombok.Data
public class Data {

    private final long id;
    @ToString.Exclude
    private final byte[] value;
}
