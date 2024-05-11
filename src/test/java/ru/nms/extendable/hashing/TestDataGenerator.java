package ru.nms.extendable.hashing;

import lombok.extern.slf4j.Slf4j;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.util.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
public class TestDataGenerator {

    public static List<Data> generateDataWithTotalSize(int bytesTotalAmount) {
        var dataList = new ArrayList<Data>();
        Random random = new Random();
        do {
            int dataLength = Math.min(random.nextInt(9, Constants.BUCKET_SIZE/3), bytesTotalAmount);
            var value = new byte[dataLength - 8];
            random.nextBytes(value);
            dataList.add(new Data(random.nextLong(0, Integer.MAX_VALUE), value));
            bytesTotalAmount -= dataLength;
        }
        while (bytesTotalAmount > 9);

        log.info("Generated {} data objects with total amount of bytes {}",
                dataList.size(),
                dataList.stream()
                        .map(Data::getValue)
                        .map(value -> value.length)
                        .mapToInt(Integer::intValue)
                        .sum());
        return dataList;
    }
}
