package ru.nms.extendable.hashing.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.nms.extendable.hashing.model.MetaData;
import ru.nms.extendable.hashing.util.Constants;
import ru.nms.extendable.hashing.util.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class MetaDataReader {



    private RandomAccessFile metaFile;

    private final String fileName;

    public List<MetaData> getMetaData() {
//log.info("About to get meta data from file {}", fileName);
        try {
            if (metaFile == null) {
                initMetaFile(fileName, "rw");
            }
            metaFile.seek(0);

            List<MetaData> metaData = new ArrayList<>();
            var elementsAmount = metaFile.readInt();
//            log.info("Elements amount is {}", elementsAmount);
            long pos = metaFile.readLong();
//            log.info("Pos is {}", pos);
//            log.info("Meta file has {} elements, elememts occupy {} bytes", elementsAmount, pos);
            for (int i = 0; i < elementsAmount; i++) {
                metaData.add(new MetaData(metaFile.readLong(), metaFile.readLong(), metaFile.readInt()));
            }
//            log.info("successfully read {} elements from meta data file {}", elementsAmount, fileName);
            return metaData;
        } catch (Exception e) {
            log.error("Didn't manage to read meta data file {} due to {}", fileName + Constants.META_POSTFIX, e.getMessage());

            return Collections.emptyList();
        }
    }

    public void writeMetaData(List<MetaData> metaDataList, boolean overwrite) {
//        log.info("About to open meta file {} for writing", Constants.PATH_TO_MAIN_DIRECTORY_WIN + fileName + Constants.META_POSTFIX);
        try {
            if (metaFile == null) {
                initMetaFile(fileName, "rw");

            }
            metaFile.seek(0);
            int elementsAmount = 0;
            long pos;
            if (overwrite) {
                pos = 12;
            } else {
                elementsAmount = metaFile.readInt();
                pos = metaFile.readLong();
            }
//            log.info("pos is {}", pos);
//            log.info("Meta file has {} elements, and has size of {} bytes", elementsAmount, pos);
            metaFile.seek(pos);
            for (MetaData metaData : metaDataList) {
                metaFile.writeLong(metaData.id());
                metaFile.writeLong(metaData.pos());
                metaFile.writeInt(metaData.len());
            }
//            log.info("Wrote meta data list with {} elements", metaDataList.size());

            long current = metaFile.getFilePointer();
            metaFile.seek(0);
            metaFile.writeInt(elementsAmount + metaDataList.size());
//            log.info("Wrote elemt amount {}", elementsAmount + metaDataList.size());
            metaFile.writeLong(Math.max(pos, current));
//            log.info("Wrote pos {}", Math.max(pos, current));
//            log.info("meta data file {} now contains {} elements, which occupy {} bytes",
//                    fileName, elementsAmount + metaDataList.size(), current);
        } catch (Exception e) {
            if (!overwrite) {
//                log.info("About to attempt overwrite metadata file {} with new data",  fileName + Constants.META_POSTFIX);
                writeMetaData(metaDataList, true);
            } else {
                log.error("Didn't manage to write metadata {} to metadata file {}",
                        metaDataList.stream().map(MetaData::toString).collect(Collectors.joining(", ")),
                        fileName + Constants.META_POSTFIX);
            }
        }
    }

    private void initMetaFile(String fileName, String mode) throws FileNotFoundException {
        log.info("Initializing meta file {}", fileName);
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + fileName + Constants.META_POSTFIX);
        //        log.info("About to open meta file {} for reading", Constants.PATH_TO_MAIN_DIRECTORY_WIN + fileName + Constants.META_POSTFIX);
        metaFile = new RandomAccessFile(file, mode);
    }

    public void close() {
        try {
            metaFile.close();
        } catch (IOException e) {
            log.error("Didn't manage to close meta data file");
        }
    }
}
