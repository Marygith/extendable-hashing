package ru.nms.extendable.hashing.service;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.StringUtils;

public class HashService {

    private final StringBuilder stringBuilder = new StringBuilder();

    public String hash(long id, int hashLen) {
        if (hashLen >= 16) {
            int a = 0;
        }
        stringBuilder.setLength(0);
        byte[] value = Longs.toByteArray(id);
        int valueLen = value.length;
        int div = hashLen / 8;
        int mod = hashLen % 8;

        var stringByte = StringUtils.leftPad(Integer.toBinaryString(value[valueLen - div - 1]), 8, '0');
        if (mod != 0) stringBuilder.append(stringByte.substring(stringByte.length() - mod));

        for (int i = div - 1; i >= 0; i--) {
            stringBuilder.append(StringUtils.leftPad(byteToBinaryString(value[valueLen - 1 - i]), 8, '0'));
        }
        return stringBuilder.reverse().toString();
    }

    String byteToBinaryString(byte b) {
        StringBuilder binaryStringBuilder = new StringBuilder();
        for (int i = 0; i < 8; i++)
            binaryStringBuilder.append(((0x80 >>> i) & b) == 0 ? '0' : '1');
        return binaryStringBuilder.toString();
    }
}
