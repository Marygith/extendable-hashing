package ru.nms.extendable.hashing;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.service.DirectoriesReader;
import ru.nms.extendable.hashing.service.HashService;
import ru.nms.extendable.hashing.service.MetaDataService;
import ru.nms.extendable.hashing.service.StorageService;
import ru.nms.extendable.hashing.util.Constants;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.nms.extendable.hashing.util.Constants.*;
@Slf4j
public class ExtendableHashingTest {

    private static StorageService service;


    @ParameterizedTest
    @ValueSource(ints = {4000, 10000, 150000})
    @DisplayName("Should successfully add and get data")
    void getAndAddLittleAmountOfDataTest(int totalBytesAmount) {
        //given
        MetaDataService.clean();
        PATH_TO_MAIN_DIRECTORY_WIN = PATH_TO_MAIN_DIRECTORY_WIN_BASE + "addAndGet" + DIR_DELIMITER + totalBytesAmount + DIR_DELIMITER;
        new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN).mkdirs();

        var dataList = TestDataGenerator.generateDataWithTotalSize(totalBytesAmount);

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
        PATH_TO_MAIN_DIRECTORY_WIN = PATH_TO_MAIN_DIRECTORY_WIN_BASE + "delete" + DIR_DELIMITER;
        new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN).mkdirs();

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
        PATH_TO_MAIN_DIRECTORY_WIN = PATH_TO_MAIN_DIRECTORY_WIN_BASE + "getFromDisk" + DIR_DELIMITER;
        new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN).mkdirs();

        var dataList = TestDataGenerator.generateDataWithTotalSize(100000);
        dataList.forEach(data -> service.putValueToStorage(data));
        service = null;

        //when-then
        var savedDirs = new DirectoriesReader();
        service = new StorageService(savedDirs, new HashService());
        log.info("Dirs global depth is {}", savedDirs.getGlobalDepth());

        for (Data data : dataList) {
            assertDoesNotThrow(() -> service.getData(data.getId()));
        }
    }

    public void initStorageService() {
        service = new StorageService(initDirs(), new HashService());
    }

    @BeforeEach
    public void setup() {
        MetaDataService.clean();
        initStorageService();
    }
    @BeforeAll
    public static void clean() {
        try {
            FileUtils.cleanDirectory(new File(Constants.PATH_TO_MAIN_DIRECTORY_WIN));
        } catch (IOException e) {
            throw new RuntimeException("Didn't manage to clean directory " + e);
        }
    }

    private static DirectoriesReader initDirs() {
        return new DirectoriesReader(1, Constants.BUCKET_SIZE);
    }
}
