package ru.nms.extendable.hashing.service;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.model.MetaData;
import ru.nms.extendable.hashing.exception.BucketWriteException;
import ru.nms.extendable.hashing.exception.MetadataSyncException;
import ru.nms.extendable.hashing.util.Constants;
import ru.nms.extendable.hashing.util.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BucketReader implements AutoCloseable {

    @Getter
    private final String fileName;

    @Getter
    private int localDepth;
    private int occupiedBytesAmount;
    private RandomAccessFile bucketFile;
    private List<MetaData> metaData;
    private List<Data> data;

    public BucketReader(String fileName) { //read existing bucket from file
        this.fileName = fileName;
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + fileName);
        try {
            bucketFile = new RandomAccessFile(file, "rw");
            localDepth = bucketFile.readInt();
            occupiedBytesAmount = bucketFile.readInt();

        } catch (Exception e) {

            if (!initBucketFile(file)) {
                throw new RuntimeException("Didn't manage to read or create bucket file" + e);
            }
            localDepth = 1;
            occupiedBytesAmount = 8;
        }
    }

    public BucketReader(List<Data> dataList, int localDepth, String link) { //create new bucket
        this.fileName = link;
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + fileName);
        try {
            bucketFile = new RandomAccessFile(file, "rw");
            this.localDepth = localDepth;
            this.occupiedBytesAmount = 8;
            writeData(dataList, Constants.BUCKET_DATA_START_POS);
        } catch (Exception e) {
            log.error("Didn't manage to create bucket file {} due to {}",
                    fileName, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean bucketCanFitNewData(int dataLength, int bucketSize) {
        return occupiedBytesAmount + dataLength <= bucketSize;
    }

    public boolean localDepthEqualsGlobal(int globalDepth) {
        return globalDepth == localDepth;
    }

    public List<MetaData> getMetadata() {
        if (metaData == null) metaData = MetaDataService.getMetaDataReader(fileName).getMetaData();
        return metaData;
    }


    public List<Data> getData() {

        if (data != null) {
            return data;
        }
        data = new ArrayList<>();
        var metadataList = getMetadata();

        for (MetaData metaData : metadataList) {
            try {
                data.add(getData(metaData));
            } catch (IOException e) {
                throw new RuntimeException("Didn't manage to extract data from bucket file");
            }

        }
        return data;
    }

    private void writeData(List<Data> dataList, long pos) {

        try {
            bucketFile.seek(pos);
            List<MetaData> metaDataList = new ArrayList<>();
            var dataSumAmount = 0;
            for (Data data : dataList) {
                metaDataList.add(new MetaData(data.getId(), bucketFile.getFilePointer(), data.getValue().length));

                bucketFile.writeLong(data.getId());
                bucketFile.write(data.getValue());
                dataSumAmount += data.getValue().length + 8;
            }
            bucketFile.seek(0);
            bucketFile.writeInt(localDepth);
            occupiedBytesAmount += dataSumAmount;
            bucketFile.writeInt(occupiedBytesAmount);

            writeMetadata(metaDataList);

        } catch (IOException e) {
            throw new BucketWriteException("", dataList.size(), localDepth);
        }
    }

    private void writeMetadata(List<MetaData> metaDataList) throws IOException {
        MetaDataService.getMetaDataReader(fileName).writeMetaData(metaDataList, false);
    }

    public void addData(Data data) {
        long pos = getMetadata().isEmpty() ? Constants.BUCKET_DATA_START_POS : getMetadata().getLast().pos() + 8 + getMetadata().getLast().len();
        writeData(List.of(data), pos);
    }

    public Data getData(MetaData metaData) throws IOException {

        bucketFile.seek(metaData.pos());
        if (bucketFile.readLong() != metaData.id()) throw new MetadataSyncException();
        byte[] value = new byte[metaData.len()];
        bucketFile.read(value);
        return new Data(metaData.id(), value);
    }

    public Integer incrementLocalDepth() {
        localDepth = localDepth + 1;
        try {
            bucketFile.seek(0);
            bucketFile.writeInt(localDepth);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return localDepth;
    }

    private boolean initBucketFile(File file) {
        try {
            bucketFile = new RandomAccessFile(file, "rw");
            return true;
        } catch (FileNotFoundException e) {
            log.error("Didn't manage to init bucket file {}", file.getName());
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        bucketFile.close();
    }
}
