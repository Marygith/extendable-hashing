package ru.nms.extendable.hashing.jmh;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.*;
import ru.nms.extendable.hashing.TestDataGenerator;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.service.DirectoriesReader;
import ru.nms.extendable.hashing.service.HashService;
import ru.nms.extendable.hashing.service.MetaDataService;
import ru.nms.extendable.hashing.service.StorageService;
import ru.nms.extendable.hashing.util.Constants;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@State(Scope.Benchmark)
public class GetDataExecutionPlan {
    @Param({"4096", "8192", "16384", "32768"})
    public int bucketSize;

    @Param({"1000", "10000", "50000", "100000", "500000"})
    public int dataAmountInBytes;

    public List<Data> dataList;

    @Setup(Level.Iteration)
    public void generateAndSaveData() {
        dataList = TestDataGenerator.generateDataWithTotalSize(dataAmountInBytes, bucketSize);
        Constants.PATH_TO_MAIN_DIRECTORY_WIN = Constants.PATH_TO_MAIN_DIRECTORY_WIN_BASE + bucketSize + "\\" + dataAmountInBytes + "\\";
        assertTrue(new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN).mkdirs());
        MetaDataService.clean();

        log.info("Dir name: {}", Constants.PATH_TO_MAIN_DIRECTORY_WIN);
        var storageService = initStorageService(bucketSize);
        dataList.forEach(storageService::putValueToStorage);
    }

/*    @TearDown(Level.Iteration)
    public void clean() {
        log.info("Cleaning...");
        try {
            FileUtils.cleanDirectory(new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN));
        } catch (IOException e) {
            throw new RuntimeException("Didn't manage to clean directory " + e);
        }
    }*/

    private StorageService initStorageService(int bucketSize) {
        return new StorageService(initDirs(bucketSize), new HashService());
    }

    private static DirectoriesReader initDirs(int bucketSize) {
        return new DirectoriesReader(1, bucketSize);
    }
}
