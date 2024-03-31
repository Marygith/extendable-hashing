package ru.nms.extendable.hashing.util;

import java.io.File;
import java.io.IOException;

public class Utils {

    public static File openFile(String fileName) {
        File file = new File(fileName);
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return file;
    }
}
