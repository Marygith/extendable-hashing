package ru.nms.extendable.hashing.service;

import lombok.extern.slf4j.Slf4j;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.exception.DataNotFoundException;
import ru.nms.extendable.hashing.util.Constants;

@Slf4j
public class StorageService {

    private final DirectoriesReader dirs;
    private final HashService hashService;

    public StorageService(DirectoriesReader dirs, HashService hashService) {
        this.dirs = dirs;
        this.hashService = hashService;
    }

    public void putValueToStorage(Data data) {
        do {
            var stringHash = hashService.hash(data.getId(), dirs.getGlobalDepth());
//            log.info("string hash {}", stringHash);
            var hash = Integer.parseInt(stringHash, 2);
            var bucketFileName = dirs.getDirsToLinks().get(hash);
//            dirs.logDirsToLinks();
            try (var bucket = new BucketReader(bucketFileName, dirs.getGlobalDepth())) {
//                log.info("bucket name: {}, local depth {}", bucketFileName, bucket.getLocalDepth());

                if (bucket.bucketCanFitNewData(data.getValue().length, dirs.getBucketSize())) {
//                    log.info("bucket can fit, adding data with id {} to bucket {}", data.getId(), bucketFileName);
                    bucket.addData(data);
                    break;
                }
                if (bucket.localDepthEqualsGlobal(dirs.getGlobalDepth())) {
//                    log.info("bucket can't fit, adding dirs");

                    dirs.addDirectories(hash, bucket, hashService);
                } else {
//                    log.info("About to split bucket {}", Integer.parseInt(dirs.getDirsToLinks().get(hash), 2));
                    dirs.splitBucket(Integer.parseInt(dirs.getDirsToLinks().get(hash), 2), bucket, hashService);
                }
            } catch (Exception e) {
                log.error("Didn't manage to put data with id {} to storage", data.getId());
                throw new RuntimeException(e);
            }

        } while (true);
    }


    public Data getData(Long id) {

//        log.info("Trying to get data with id {}", id);
        var hash = hashService.hash(id, dirs.getGlobalDepth());
        var intHash = Integer.parseInt(hash, 2);
//        log.info("Hashed id {}. String hash: {}, int hash: {}", id, hash, intHash);
        var bucketFileName = dirs.getDirsToLinks().get(intHash);
//        dirs.logDirsToLinks();
        try (var bucket = new BucketReader(bucketFileName, dirs.getGlobalDepth())) {
            var metadataList = bucket.getMetadata();
//            log.info("metadata from bucket {} is {}", bucketFileName, metadataList.toString());

            var meta = metadataList.parallelStream().filter(m -> m.id() == id).findAny()
                    .orElseThrow(() -> new DataNotFoundException(id));
            return bucket.getData(meta);
        } catch (Exception e) {
            log.error("Didn't manage to get data with id {}", id);
            throw new RuntimeException(e);
        }
    }

    public void deleteData(long id) {
//        log.info("About to delete data with id {}", id);
        var hash = hashService.hash(id, dirs.getGlobalDepth());
        var intHash = Integer.parseInt(hash, 2);
        var bucketFileName = dirs.getDirsToLinks().get(intHash);

        try (var bucket = new BucketReader(bucketFileName, dirs.getGlobalDepth())) {
            var metadataList = bucket.getMetadata();
            var metaWithoutDeleted = metadataList.parallelStream().filter(m -> m.id() != id).toList();
            MetaDataService.getMetaDataReader(bucketFileName).writeMetaData(metaWithoutDeleted, true);
        } catch (Exception e) {
            log.error("Didn't manage to delete meta data with id {} from file {} due to {}",
                    id, bucketFileName + Constants.META_POSTFIX, e.getMessage());
        }
    }
}
