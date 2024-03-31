package ru.nms.extendable.hashing.service;

import lombok.extern.slf4j.Slf4j;
import ru.nms.extendable.hashing.model.Data;
import ru.nms.extendable.hashing.exception.DataNowFoundException;
import ru.nms.extendable.hashing.exception.DataWriteException;
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
        log.info("Trying to put new data with id {}", data.getId());

        var hash = hashService.hash(data.getId(), dirs.getGlobalDepth());
        var intHash = Integer.parseInt(hash, 2);
        log.info("Hashed id {}. String hash: {}, int hash: {}", data.getId(), hash, intHash);

        var bucketFileName = dirs.getDirsToLinks().get(intHash);
        try (var bucket = new BucketReader(bucketFileName, dirs.getGlobalDepth())) {

            if (!bucket.bucketCanFitNewData(data.getValue().length + 8)) {
                log.info("Bucket {} can't fit new data", bucketFileName);
                if (bucket.localDepthEqualsGlobal(dirs.getGlobalDepth())) {
                    dirs.addDirectories(intHash, bucket, hashService);
                } else {
                    dirs.splitBucket(intHash, bucket, hashService);
                }
                log.info("Attempting to put data to storage again");
                putValueToStorage(data);
            } else {
                bucket.addData(data);
            }
        } catch (Exception e) {
            throw new DataWriteException();
        }
    }

    public Data getData(Long id) {
        log.info("Trying to get data with id {}", id);
        var hash = hashService.hash(id, dirs.getGlobalDepth());
        var intHash = Integer.parseInt(hash, 2);
        log.info("Hashed id {}. String hash: {}, int hash: {}", id, hash, intHash);
        var bucketFileName = dirs.getDirsToLinks().get(intHash);

        try (var bucket = new BucketReader(bucketFileName, dirs.getGlobalDepth())) {
            var metadataList = bucket.getMetadata();
            var meta = metadataList.parallelStream().filter(m -> m.id() == id).findAny()
                    .orElseThrow(() -> new DataNowFoundException(id));
            return bucket.getData(meta);
        } catch (Exception e) {
            throw new DataWriteException();
        }
    }

    public boolean deleteData(long id) {
        log.info("Trying to get data with id {}", id);
        var hash = hashService.hash(id, dirs.getGlobalDepth());
        var intHash = Integer.parseInt(hash, 2);
        var bucketFileName = dirs.getDirsToLinks().get(intHash);

        try (var bucket = new BucketReader(bucketFileName, dirs.getGlobalDepth())) {
            var metadataList = bucket.getMetadata();
            var metaWithoutDeleted = metadataList.parallelStream().filter(m -> m.id() != id).toList();
            MetaDataReader.writeMetaData(metaWithoutDeleted, bucketFileName, true);
            return true;
        } catch (Exception e) {
            log.error("Didn't manage to delete meta data with id {} from file {} due to {}",
                    id, bucketFileName + Constants.META_POSTFIX, e.getMessage());
            return false;
        }
    }
}
