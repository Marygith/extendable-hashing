package ru.nms.extendable.hashing.service;

import lombok.extern.slf4j.Slf4j;
import ru.nms.extendable.hashing.model.MetaData;
import ru.nms.extendable.hashing.util.Constants;
import ru.nms.extendable.hashing.util.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MetaDataReader {


    public static List<MetaData> getMetaData(String fileName) {
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY + fileName + Constants.META_POSTFIX);
        log.info("About to open meta file {}", Constants.PATH_TO_MAIN_DIRECTORY + fileName + Constants.META_POSTFIX);
        try (RandomAccessFile metaFile = new RandomAccessFile(file, "rw");
        ) {
            List<MetaData> metaData = new ArrayList<>();
            var elementsAmount = metaFile.readInt();
            long pos = metaFile.readLong();
            log.info("Meta file has {} elements, elememts occupy {} bytes", elementsAmount, pos);
            for (int i = 0; i < elementsAmount; i++) {
                metaData.add(new MetaData(metaFile.readLong(), metaFile.readLong(), metaFile.readInt()));
            }
            return metaData;
        } catch (Exception e) {
            log.error("Didn't manage to read meta data file {} due to {}" , fileName + Constants.META_POSTFIX, e.getMessage());

            return Collections.emptyList();
        }
    }

    public static void writeMetaData(List<MetaData> metaDataList, String fileName, boolean overwrite) {
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY + fileName + Constants.META_POSTFIX);
        log.info("About to open meta file {}", Constants.PATH_TO_MAIN_DIRECTORY + fileName + Constants.META_POSTFIX);
        try (RandomAccessFile metaFile = new RandomAccessFile(file, "rw");
        ) {
            int elementsAmount = 0;
            long pos;
            if(overwrite) {
                pos = 12;
            } else {
                elementsAmount = metaFile.readInt();
                pos = metaFile.readLong();
            }
            log.info("Meta file has {} elements, elememts occupy {} bytes", elementsAmount, pos);
            metaFile.seek(pos);
            for (MetaData metaData : metaDataList) {
                metaFile.writeLong(metaData.id());
                metaFile.writeLong(metaData.pos());
                metaFile.writeInt(metaData.len());
            }

            long current = metaFile.getFilePointer();
            metaFile.seek(0);
            metaFile.writeInt(elementsAmount + metaDataList.size());
            metaFile.writeLong(current);
            log.info("meta data file {} now contains {} elements, which occupy {} bytes",
                    fileName, elementsAmount + metaDataList.size(), current);
        } catch (Exception e) {
            log.error("Didn't manage to write metadata {} to metadata file {} due to " + e.getMessage(),
                    metaDataList.stream().map(MetaData::toString).collect(Collectors.joining(", ")),
                    fileName + Constants.META_POSTFIX);
            if(!overwrite) {
                log.info("About to attempt overwrite metadata file {} with new data",  fileName + Constants.META_POSTFIX);
                writeMetaData(metaDataList, fileName, true);
            }
        }
    }

}
