package ru.nms.extendable.hashing.service;

import java.util.HashMap;
import java.util.Map;

public class MetaDataService {

    private static final Map<String, MetaDataReader> META_FILES = new HashMap<>();

    public static MetaDataReader getMetaDataReader(String fileName) {
        if(META_FILES.containsKey(fileName)) return META_FILES.get(fileName);
        else {
            var reader = new MetaDataReader(fileName);
            META_FILES.put(fileName, reader);
            return reader;
        }
    }

    public static void deleteMetaDataReader(String fileName) {
        var reader = META_FILES.get(fileName);
        reader.close();
        META_FILES.remove(fileName);
    }

    public static void clean() {
        META_FILES.clear();
    }
}
