package ru.nms.extendable.hashing.jmh;

import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import ru.nms.extendable.hashing.service.DirectoriesReader;
import ru.nms.extendable.hashing.service.HashService;
import ru.nms.extendable.hashing.service.StorageService;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Slf4j
@Fork(value = 1, warmups = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class GetDataBenchmark {


    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testGet(GetDataExecutionPlan plan) {
        var storageService = initStorageService();

        plan.dataList.forEach(data -> assertDoesNotThrow(() -> storageService.getData(data.getId())));
    }

    private StorageService initStorageService() {
        return new StorageService(initDirs(), new HashService());
    }


    private static DirectoriesReader initDirs() {
        return new DirectoriesReader();
    }
}
