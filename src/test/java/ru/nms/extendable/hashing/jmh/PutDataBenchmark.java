package ru.nms.extendable.hashing.jmh;


import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import ru.nms.extendable.hashing.service.DirectoriesReader;
import ru.nms.extendable.hashing.service.HashService;
import ru.nms.extendable.hashing.service.MetaDataService;
import ru.nms.extendable.hashing.service.StorageService;
import ru.nms.extendable.hashing.util.Constants;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Fork(value = 1, warmups = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class PutDataBenchmark {


    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testPut(PutDataExecutionPlan plan) {
        Constants.PATH_TO_MAIN_DIRECTORY_WIN = Constants.PATH_TO_MAIN_DIRECTORY_WIN_BASE + plan.bucketSize + "\\" + plan.dataAmountInBytes + "\\" + System.currentTimeMillis() + "\\";
        assertTrue(new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN).mkdirs());
        MetaDataService.clean();

        log.info("Dir name: {}", Constants.PATH_TO_MAIN_DIRECTORY_WIN);
        var storageService = initStorageService(plan.bucketSize);
        plan.dataList.forEach(storageService::putValueToStorage);
    }

    private StorageService initStorageService(int bucketSize) {
        return new StorageService(initDirs(bucketSize), new HashService());
    }

    private static DirectoriesReader initDirs(int bucketSize) {
        return new DirectoriesReader(1, bucketSize);
    }
}
