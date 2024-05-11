package ru.nms.extendable.hashing.jmh;

import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import ru.nms.extendable.hashing.TestDataGenerator;
import ru.nms.extendable.hashing.model.Data;

import java.util.List;

@Slf4j
@State(Scope.Benchmark)
public class PutDataExecutionPlan {

    @Param({ "4096",  "8192",  "16384", "32768"})
    public int bucketSize;

    @Param({ "1000",  "10000",  "50000", "100000", "500000"})
    public int dataAmountInBytes;

    public List<Data> dataList;

    @Setup(Level.Iteration)
    public void generateData() {
        dataList = TestDataGenerator.generateDataWithTotalSize(dataAmountInBytes, bucketSize);
    }
}
