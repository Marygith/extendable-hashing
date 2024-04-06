package ru.nms.extendable.hashing.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.util.Constants;
import ru.nms.extendable.hashing.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@Slf4j
public class DirectoriesReader {

    private int globalDepth;

    private int bucketSize;

    private final HashMap<Integer, String> dirsToLinks = new HashMap<>();

    public DirectoriesReader() {// read from existing file
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY + Constants.DIRECTORIES_FILE_NAME);
        try (RandomAccessFile directoriesFile = new RandomAccessFile(file, "r")) {
            globalDepth = directoriesFile.readInt();
            bucketSize = directoriesFile.readInt();
            for (int i = 0; i < Math.pow(2, globalDepth); i++) {
                dirsToLinks.put(i, directoriesFile.readInt() + "");
            }
        } catch (Exception e) {
            System.err.println("Didn't manage to read directories file due to " + e.getMessage());
        }
    }

    public DirectoriesReader(int globalDepth) {//create directories file
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY + Constants.DIRECTORIES_FILE_NAME);
        try (RandomAccessFile directoriesFile = new RandomAccessFile(file, "rw")) {
            this.globalDepth = globalDepth;
            bucketSize = Constants.BUCKET_SIZE;
            directoriesFile.writeInt(globalDepth);
            directoriesFile.writeInt(bucketSize);
            for (int i = 0; i < Math.pow(2, globalDepth); i++) {
                dirsToLinks.put(i, i + "");
                directoriesFile.writeInt(i);
            }
        } catch (Exception e) {
            System.err.println("Didn't manage to create directories file due to " + e.getMessage());
        }
    }


    public void addDirectories(int overflowedBucket, BucketReader bucket, HashService hashService) {
        log.info("Adding directories, global depth {}, overflowed bucket {}", globalDepth, overflowedBucket);
        globalDepth++;
        dirsToLinks.forEach((dir, link) ->
        {
            if (dir == overflowedBucket) {
                splitBucket(dir, bucket, hashService);
                deleteOldBucket(link);
            } else {
                var newDirBase = dir << 1;
                dirsToLinks.put(newDirBase, link);
                dirsToLinks.put(newDirBase + 1, link);
            }

        });

        dirsToLinks.entrySet().removeIf(entry -> Integer.toBinaryString(entry.getKey()).length() < globalDepth);

        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY + Constants.DIRECTORIES_FILE_NAME);
        try (RandomAccessFile directoriesFile = new RandomAccessFile(file, "rw")) {
            directoriesFile.writeInt(globalDepth);
            directoriesFile.readInt();
            for (int i = 0; i < Math.pow(2, globalDepth); i++) {
                directoriesFile.writeInt(Integer.parseInt(dirsToLinks.get(i)));
            }
        } catch (Exception e) {
            throw new RuntimeException("Didn't manage to write directories file due to " + e.getMessage());
        }
    }


    public void splitBucket(int oldDir, BucketReader bucket, HashService hashService) {
        log.info("Splitting bucket {}", oldDir);

        Map<Integer, List<Data>> bucketToData = new HashMap<>();
        var newBucket1 = oldDir << 1;
        var newBucket2 = (oldDir << 1) + 1;
        log.info("New directories: {} and {}", newBucket1, newBucket2);

        bucketToData.put(newBucket1, new ArrayList<>());
        bucketToData.put(newBucket2, new ArrayList<>());

        var dataList = bucket.getData();
        for (Data data : dataList) {
            var hash = Integer.parseInt(hashService.hash(data.getId(), globalDepth));
            bucketToData.get(hash).add(data);
        }
        logBucketMap(bucketToData);
        if (bucketToData.get(newBucket1).isEmpty() || bucketToData.get(newBucket1).isEmpty()) {
            log.warn("Didn't manage to split bucket with global depth {}, splitting again", globalDepth);
            addDirectories(oldDir, bucket, hashService);
            return;
        }
        bucketToData.forEach((dir, data) -> {
            try (var newBucket = new BucketReader(data, globalDepth, dir)) {
                dirsToLinks.put(dir, newBucket.getFileName());
            } catch (Exception e) {
                log.error("Didn't manage to create new bucket");
                throw new RuntimeException(e);
            }
        });
    }

    private void logBucketMap(Map<Integer, List<Data>> bucketToData) {
        log.info("Split old bucket into new buckets: {}", bucketToData.entrySet().stream()
                .map((e) -> "\nbucket: " + e.getKey() + ", \n\tvalues : "
                        + e.getValue().stream().map(Data::toString)
                        .collect(Collectors.joining(", ", "[", "]")))
                .collect(Collectors.joining("; ")));
    }

    private void deleteOldBucket(String link) {
        try {
            log.info("Trying to delete old bucket {}", link);
            Files.delete(Path.of(Constants.PATH_TO_MAIN_DIRECTORY + link));
        } catch (IOException e) {
            log.error("Didn't manage to delete file {}", link);
        }
    }
}
