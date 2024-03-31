package ru.nms.extendable.hashing;

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

public class ExtendableHashingTest {

    private static StorageService service;


    @Test
    @DisplayName("Should successfully add data")
    void test() {
        //given
        var dataList = TestDataGenerator.generateDataWithTotalSize(4000);

        //when
        dataList.forEach(data -> service.putValueToStorage(data));

        //then
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
            FileUtils.cleanDirectory(new File(Constants.PATH_TO_MAIN_DIRECTORY));
        } catch (IOException e) {
            throw new RuntimeException("Didn't manage to clean directiry"+e);
        }
    }

    private static DirectoriesReader initDirs() {
        return new DirectoriesReader(1);
    }
}
