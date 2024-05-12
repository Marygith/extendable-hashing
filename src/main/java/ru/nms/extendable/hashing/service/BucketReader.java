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
    private int localDepth;

    private int occupiedBytesAmount;

    private RandomAccessFile bucketFile;

    private List<MetaData> metaData;

    private List<Data> data;

    @Getter
    private final String fileName;

    public BucketReader(String fileName, int globalDepth) { //read existing bucket from file
        this.fileName = fileName;
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + fileName);
        try {
            bucketFile = new RandomAccessFile(file, "rw");
            localDepth = bucketFile.readInt();
            occupiedBytesAmount = bucketFile.readInt();
//            log.info("Successfully read bucket {}, local depth {}, occupied bytes amount {}", file, localDepth, occupiedBytesAmount);
        } catch (Exception e) {

            if(!initBucketFile(file)) {
                throw  new RuntimeException("Didn't manage to read or create bucket file" + e);
            }
            localDepth = 1;
            occupiedBytesAmount = 8;
//            log.info("Created new bucket file {}, local depth {}, occupied bytes amount {}",
//                    fileName, localDepth, occupiedBytesAmount);


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
//            log.error("Didn't manage to read bucket file {} due to {}",
//                    fileName, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean bucketCanFitNewData(int dataLength, int bucketSize) {
//        log.info("Checking whether bucket can fit: new data has {} bytes, bucket has {} bytes occupied, {} is max bytes amount",
//                dataLength, occupiedBytesAmount, Constants.BUCKET_SIZE);
        return occupiedBytesAmount + dataLength <= bucketSize;
    }

    public boolean localDepthEqualsGlobal(int globalDepth) {
//        log.info("Local depth: {}, global depth: {}", localDepth, globalDepth);
        return globalDepth == localDepth;
    }

    public List<MetaData> getMetadata() {
        if (metaData == null) {
//            log.info("metadata is not cached, reading from file");
            metaData = MetaDataService.getMetaDataReader(fileName).getMetaData();}
        return metaData;
    }


    public List<Data> getData() {

//        log.info("About to get all data from bucket {}", fileName);
        if (data != null) {
            return data;
        }
        data = new ArrayList<>();
        var metadataList = getMetadata();
//        log.info("Reading data from bucket {}, meta data : {}", fileName, metadataList);
        for (MetaData metaData : metadataList) {
            try {
                data.add(getData(metaData));
//                log.info("Data with id {}, pos {} and value len {} successfully read", metaData.id(), metaData.pos(), metaData.len());
            } catch (IOException e) {
//                log.error("Error occurred while reading data with id {}, pos {} and value len {}", metaData.id(), metaData.pos(), metaData.len());
                throw new RuntimeException("Didn't manage to extract data from bucket file");
            }

        }
        return data;
    }

    private void writeData(List<Data> dataList, long pos) {
//        log.info("Trying to write data to bucket {}, pos {}, data size {}",
//                fileName, pos, dataList.size());
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
//            log.info("Successfully wrote {} elements to bucket {}, which now has {} bytes occupied, local depth: {}",
//                    dataList.size(), fileName, occupiedBytesAmount, localDepth);
            writeMetadata(metaDataList);

        } catch (IOException e) {
            throw new BucketWriteException("", dataList.size(), localDepth);
        }
    }

    private void writeMetadata(List<MetaData> metaDataList) throws IOException {
        MetaDataService.getMetaDataReader(fileName).writeMetaData(metaDataList, false);
    }

    public void addData(Data data) {
//        log.info("About to add data with id {} to bucket {}", data.getId(), fileName);
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

/*    public Data deleteDataWithActualRemoval(MetaData metaData) throws IOException {
        var data = getData();
        var dataToRewrite = data.stream().dropWhile(d -> d.getId() != (metaData.id())).toList();
        var dataToDelete = dataToRewrite.removeFirst();
        //todo
        writeData(dataToRewrite, metaData.pos());
        if (bucketFile.readLong() != metaData.id()) throw new MetadataSyncException();
        byte[] value = new byte[metaData.len()];
        return new Data(metaData.id(), value);
    }*/

    private boolean initBucketFile(File file) {
        try {
            bucketFile = new RandomAccessFile(file, "rw");
            return true;
        } catch (FileNotFoundException e) {
//            log.error("Didn't manage to init bucket file {}", file.getName());
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        bucketFile.close();
    }
}
