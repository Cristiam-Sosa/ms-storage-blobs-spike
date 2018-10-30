package com.innocv.msStorageBlobSpike.services;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface BlobStorageService {
    BlobStorageService selectContainer(String containerName);

    CompletableFuture<Boolean> upload(File file) throws Exception;

    CompletableFuture<List<String>> listBlobs();

    CompletableFuture<File> download(String filename) throws Exception;

    CompletableFuture<Boolean> delete(String filename);
}
