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

    private final String fileName;

    private RandomAccessFile metaFile;

    public List<MetaData> getMetaData() {

        try {
            if (metaFile == null) {
                initMetaFile(fileName);
            }
            metaFile.seek(0);

            List<MetaData> metaData = new ArrayList<>();
            var elementsAmount = metaFile.readInt();
            long pos = metaFile.readLong();
            for (int i = 0; i < elementsAmount; i++) {
                metaData.add(new MetaData(metaFile.readLong(), metaFile.readLong(), metaFile.readInt()));
            }

            return metaData;
        } catch (Exception e) {
            log.error("Didn't manage to read meta data file {} due to {}", fileName + Constants.META_POSTFIX, e.getMessage());

            return Collections.emptyList();
        }
    }

    public void writeMetaData(List<MetaData> metaDataList, boolean overwrite) {

        try {
            if (metaFile == null) {
                initMetaFile(fileName);

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

            metaFile.seek(pos);
            for (MetaData metaData : metaDataList) {
                metaFile.writeLong(metaData.id());
                metaFile.writeLong(metaData.pos());
                metaFile.writeInt(metaData.len());
            }

            long current = metaFile.getFilePointer();
            metaFile.seek(0);
            metaFile.writeInt(elementsAmount + metaDataList.size());
            metaFile.writeLong(Math.max(pos, current));

        } catch (Exception e) {
            if (!overwrite) {
                writeMetaData(metaDataList, true);
            } else {
                log.error("Didn't manage to write metadata {} to metadata file {}",
                        metaDataList.stream().map(MetaData::toString).collect(Collectors.joining(", ")),
                        fileName + Constants.META_POSTFIX);
            }
        }
    }

    private void initMetaFile(String fileName) throws FileNotFoundException {
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + fileName + Constants.META_POSTFIX);
        metaFile = new RandomAccessFile(file, "rw");
    }

    public void close() {
        try {
            metaFile.close();
        } catch (IOException e) {
            log.error("Didn't manage to close meta data file");
        }
    }
}
