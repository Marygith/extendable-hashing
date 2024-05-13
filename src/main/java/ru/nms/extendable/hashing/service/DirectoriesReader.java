package ru.nms.extendable.hashing.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
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

    private final Map<Integer, String> dirsToLinks = new HashMap<>();
    private final Map<Integer, String> bufferMap = new HashMap<>();

    private int globalDepth;
    private int bucketSize;

    public DirectoriesReader() {// read from existing file
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + Constants.DIRECTORIES_FILE_NAME);
        try (RandomAccessFile directoriesFile = new RandomAccessFile(file, "r")) {
            globalDepth = directoriesFile.readInt();
            bucketSize = directoriesFile.readInt();
            for (int i = 0; i < Math.pow(2, globalDepth); i++) {
                var link = directoriesFile.readInt();
                var linkLength = directoriesFile.readInt();
                dirsToLinks.put(i, StringUtils.leftPad(Integer.toBinaryString(link), linkLength, '0'));
            }
        } catch (Exception e) {
            System.err.println("Didn't manage to read directories file due to " + e.getMessage());
        }
    }

    public DirectoriesReader(int globalDepth, int bucketSize) {//create directories file
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + Constants.DIRECTORIES_FILE_NAME);
        try (RandomAccessFile directoriesFile = new RandomAccessFile(file, "rw")) {
            this.globalDepth = globalDepth;
            this.bucketSize = bucketSize;
            directoriesFile.writeInt(globalDepth);
            directoriesFile.writeInt(bucketSize);
            for (int i = 0; i < Math.pow(2, globalDepth); i++) {
                dirsToLinks.put(i, StringUtils.leftPad(Integer.toBinaryString(i), globalDepth, '0'));
                directoriesFile.writeInt(i);
                directoriesFile.writeInt(globalDepth);
            }
        } catch (Exception e) {
            System.err.println("Didn't manage to create directories file due to " + e.getMessage());
        }
    }


    public void addDirectories(int overflowedBucket, BucketReader bucket, HashService hashService) {
        globalDepth++;

        var iterator = dirsToLinks.entrySet().iterator();
        bufferMap.clear();
        while (iterator.hasNext()) {
            var entry = iterator.next();

            if (entry.getKey() != overflowedBucket) {
                var newDirBase = entry.getKey() << 1; //create two new buckets from old one
                bufferMap.put(newDirBase, entry.getValue()); //assign old link to new bucket
                bufferMap.put(newDirBase + 1, entry.getValue());
            }
        }

        var link = dirsToLinks.get(overflowedBucket);
        dirsToLinks.clear();
        dirsToLinks.putAll(bufferMap); //overwrite old directories

        splitBucket(overflowedBucket, bucket, hashService);
        deleteOldBucket(link);

    }


    public void splitBucket(int oldDir, BucketReader bucket, HashService hashService) {
        var localDepth = bucket.incrementLocalDepth(); //increment local depth

        Map<Integer, List<Data>> bucketToData = new HashMap<>();
        var newBucket1 = oldDir << 1;
        var newBucket2 = (oldDir << 1) + 1;

        bucketToData.put(newBucket1, new ArrayList<>());
        bucketToData.put(newBucket2, new ArrayList<>());
        var dataList = bucket.getData(); //getting data from old bucket
        for (Data data : dataList) {
            var hash = Integer.parseInt(hashService.hash(data.getId(), localDepth), 2);
            bucketToData.get(hash).add(data); //rehash data and distribute between two new buckets
        }

        bucketToData.forEach((dir, data) -> {
            var dirCombinations = findAllCombinationsForDir(dir, localDepth);

            try (var newBucket = new BucketReader(data, localDepth, StringUtils.leftPad(Integer.toBinaryString(dir), localDepth, '0'))) {
                dirCombinations.forEach((combination) -> dirsToLinks.put(combination, newBucket.getFileName()));
            } catch (Exception e) {
                log.error("Didn't manage to create new bucket");
                throw new RuntimeException(e);
            }
        });

        saveDirs();
        if (bucketToData.get(newBucket1).isEmpty()) {
            if (localDepth < globalDepth) {

                log.warn("Didn't manage to split bucket with local depth {}, and global depth {}, splitting again",
                        localDepth, globalDepth);
                splitBucket(newBucket2, bucket, hashService);
                return;
            }
            log.warn("Didn't manage to split bucket with global depth {}, splitting again", globalDepth);
            addDirectories(newBucket2, bucket, hashService);
        }
        else if (bucketToData.get(newBucket2).isEmpty()) {
            if (localDepth < globalDepth) {
                log.warn("Didn't manage to split bucket with local depth {}, and global depth {}, splitting again",
                        localDepth, globalDepth);
                splitBucket(newBucket1, bucket, hashService);
                return;
            }
            log.warn("Didn't manage to split bucket with global depth {}, splitting again", globalDepth);
            addDirectories(newBucket1, bucket, hashService);
        }
    }


    private void deleteOldBucket(String link) {
        try {
            MetaDataService.deleteMetaDataReader(link);
            Files.delete(Path.of(Constants.PATH_TO_MAIN_DIRECTORY_WIN + link + Constants.META_POSTFIX));
        } catch (IOException e) {
            log.error("Didn't manage to delete file {}", link);
        }
    }

    private List<Integer> findAllCombinationsForDir(int dir, int localDepth) {
        if (localDepth == globalDepth) return List.of(dir);
        var list = new ArrayList<Integer>();
        var min = dir << (globalDepth - localDepth);
        var max = dir;
        for (int i = 0; i < (globalDepth - localDepth); i++) {
            max = (max << 1) + 1;
        }
        for (int i = min; i <= max; i++) {
            list.add(i);
        }
        return list;
    }

    private void saveDirs() {
        File file = Utils.openFile(Constants.PATH_TO_MAIN_DIRECTORY_WIN + Constants.DIRECTORIES_FILE_NAME);
        try (RandomAccessFile directoriesFile = new RandomAccessFile(file, "rw")) {
            directoriesFile.writeInt(globalDepth);
            directoriesFile.writeInt(bucketSize);

            for (int i = 0; i < Math.pow(2, globalDepth); i++) {
                directoriesFile.writeInt(Integer.parseInt(dirsToLinks.get(i), 2));
                directoriesFile.writeInt(dirsToLinks.get(i).length());

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void logDirsToLinks() {
        log.info("Dir to links map : {}", dirsToLinks.entrySet().stream()
                .map(entry -> "\ndir: " + entry.getKey() + ", link: " + entry.getValue()).collect(Collectors.joining(";")));
    }

    private void logBucketMap(Map<Integer, List<Data>> bucketToData) {
        log.info("Split old bucket into new buckets: {}", bucketToData.entrySet().stream()
                .map((e) -> "\nbucket: " + StringUtils.leftPad(Integer.toBinaryString(e.getKey()), globalDepth, '0') + ", \n\tvalues : "
                        + e.getValue().stream().map(Data::toString)
                        .collect(Collectors.joining(", ", "[", "]")))
                .collect(Collectors.joining("; ")));
    }

}
