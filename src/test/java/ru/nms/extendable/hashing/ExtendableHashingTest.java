package ru.nms.extendable.hashing;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.service.DirectoriesReader;
import ru.nms.extendable.hashing.service.HashService;
import ru.nms.extendable.hashing.service.StorageService;
import ru.nms.extendable.hashing.util.Constants;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public class ExtendableHashingTest {

    private static StorageService service;


    @Test
    @DisplayName("Should successfully add and get data with total amount under bucket size")
    void getAndAddLittleAmountOfDataTest() {
        //given
        var dataList = TestDataGenerator.generateDataWithTotalSize(4000);

        //when
        dataList.forEach(data -> service.putValueToStorage(data));

        //then
        for (Data data : dataList) {
            assertDoesNotThrow(() -> service.getData(data.getId()));
        }
    }

    @Test
    @DisplayName("Should successfully add and get data with total amount above bucket size")
    void getAndAddSignificantAmountOfDataTest() {
        //given
        var dataList = TestDataGenerator.generateDataWithTotalSize(10000);

        //when
        dataList.forEach(data -> service.putValueToStorage(data));

        //then
        for (Data data : dataList) {
            assertDoesNotThrow(() -> service.getData(data.getId()));
        }
    }

    @Test
    @DisplayName("Should successfully add and get data with total amount much above bucket size")
    void getAndAddLargeAmountOfDataTest() {
        //given
        var dataList = TestDataGenerator.generateDataWithTotalSize(150000);

        //when
        dataList.forEach(data -> service.putValueToStorage(data));

        //then
        for (Data data : dataList) {
            assertDoesNotThrow(() -> service.getData(data.getId()));
        }
    }

    @Test
    @DisplayName("Should delete data by id")
    void deleteDataById() {
        //given
        var dataList = TestDataGenerator.generateDataWithTotalSize(150000);
        dataList.forEach(data -> service.putValueToStorage(data));
        var data = dataList.getFirst();
        service.deleteData(data.getId());

        //when-then
        assertThrows(RuntimeException.class, () -> service.getData(data.getId()));
    }

    @Test
    @DisplayName("Should get data from disk")
    void getDataFromDisk() {
        //given
        var dataList = TestDataGenerator.generateDataWithTotalSize(100000);
        dataList.forEach(data -> service.putValueToStorage(data));
        service = null;

        //when-then
        var savedDirs = new DirectoriesReader();
        service = new StorageService(savedDirs, new HashService());
        log.info("Dirs global depth is {}", savedDirs.getGlobalDepth());
        savedDirs.logDirsToLinks();
        for (Data data : dataList) {
            assertDoesNotThrow(() -> service.getData(data.getId()));
        }
    }


    @BeforeAll
    public static void initStorageService() {
        service = new StorageService(initDirs(), new HashService());
    }

    @BeforeEach
    public void clean() {
        try {
            FileUtils.cleanDirectory(new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN));
        } catch (IOException e) {
            throw new RuntimeException("Didn't manage to clean directory "+e);
        }
    }

    private static DirectoriesReader initDirs() {
        return new DirectoriesReader(1, Constants.BUCKET_SIZE);
    }
}
