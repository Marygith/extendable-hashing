package ru.nms.extendable.hashing.service;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.StringUtils;

public class HashService {

    private final StringBuilder stringBuilder = new StringBuilder();
    public String hash(long id, int hashLen) {
        stringBuilder.setLength(0);
        byte[] value = Longs.toByteArray(id);
        int valueLen = value.length;
        int div = hashLen / 8;
        int mod = hashLen % 8;
        var stringByte = StringUtils.leftPad(Integer.toBinaryString(value[valueLen - div - 1]), 8, '0');
        if(mod != 0) stringBuilder.append(stringByte.substring(stringByte.length() - mod));
        for (int i = 0; i < div; i++) {
            stringBuilder.append(Integer.toBinaryString(value[valueLen - 1 - div + i]));
        }
        return stringBuilder.toString();
    }
}
