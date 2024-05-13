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

            var hash = Integer.parseInt(stringHash, 2);
            var bucketFileName = dirs.getDirsToLinks().get(hash);

            try (var bucket = new BucketReader(bucketFileName)) {

                if (bucket.bucketCanFitNewData(data.getValue().length, dirs.getBucketSize())) {
                    bucket.addData(data);
                    break;
                }
                if (bucket.localDepthEqualsGlobal(dirs.getGlobalDepth())) {
                    dirs.addDirectories(hash, bucket, hashService);
                } else {
                    dirs.splitBucket(Integer.parseInt(dirs.getDirsToLinks().get(hash), 2), bucket, hashService);
                }
            } catch (Exception e) {
                log.error("Didn't manage to put data with id {} to storage", data.getId());
                throw new RuntimeException(e);
            }

        } while (true);
    }


    public Data getData(Long id) {

        var hash = hashService.hash(id, dirs.getGlobalDepth());
        var intHash = Integer.parseInt(hash, 2);
        var bucketFileName = dirs.getDirsToLinks().get(intHash);
        try (var bucket = new BucketReader(bucketFileName)) {
            var metadataList = bucket.getMetadata();

            var meta = metadataList.parallelStream().filter(m -> m.id() == id).findAny()
                    .orElseThrow(() -> new DataNotFoundException(id));
            return bucket.getData(meta);
        } catch (Exception e) {
            log.error("Didn't manage to get data with id {}", id);
            throw new RuntimeException(e);
        }
    }

    public void deleteData(long id) {
        var hash = hashService.hash(id, dirs.getGlobalDepth());
        var intHash = Integer.parseInt(hash, 2);
        var bucketFileName = dirs.getDirsToLinks().get(intHash);

        try (var bucket = new BucketReader(bucketFileName)) {
            var metadataList = bucket.getMetadata();
            var metaWithoutDeleted = metadataList.parallelStream().filter(m -> m.id() != id).toList();
            MetaDataService.getMetaDataReader(bucketFileName).writeMetaData(metaWithoutDeleted, true);
        } catch (Exception e) {
            log.error("Didn't manage to delete meta data with id {} from file {} due to {}",
                    id, bucketFileName + Constants.META_POSTFIX, e.getMessage());
        }
    }
}
