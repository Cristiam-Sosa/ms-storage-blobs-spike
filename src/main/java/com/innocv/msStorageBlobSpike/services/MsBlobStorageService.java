package com.innocv.msStorageBlobSpike.services;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.*;
import com.microsoft.rest.v2.Context;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class MsBlobStorageService implements BlobStorageService {

    @Autowired
    private Environment environment;

    private ServiceURL serviceURL;
    private ContainerURL containerURL;

    @PostConstruct
    void connectWithAzure() throws Exception {
        String accountName = environment.getProperty("ms.blobStorage.accountName");
        String accountKey = environment.getProperty("ms.blobStorage.accountKey");
        SharedKeyCredentials credentials = new SharedKeyCredentials(accountName, accountKey);
        serviceURL = new ServiceURL(new URL("https://" + accountName + ".blob.core.windows.net"), StorageURL.createPipeline(credentials, new PipelineOptions()));
    }

    @Override
    public BlobStorageService selectContainer(String containerName) {
        containerURL = serviceURL.createContainerURL(containerName);
        try {
            containerURL.create(Metadata.NONE, PublicAccessType.CONTAINER, Context.NONE).blockingGet();
        } catch (Exception exception) {
        }
        return this;
    }

    @Override
    @Async
    public CompletableFuture<Boolean> upload(File file) throws Exception {
        BlockBlobURL blockBlobURL = containerURL.createBlockBlobURL(file.getName());

        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(file.toPath());

        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        TransferManager.uploadFileToBlockBlob(fileChannel, blockBlobURL, TransferManager.BLOB_DEFAULT_DOWNLOAD_BLOCK_SIZE, TransferManagerUploadToBlockBlobOptions.DEFAULT).subscribe(new SingleObserver<CommonRestResponse>() {
            @Override
            public void onSubscribe(Disposable disposable) {
            }

            @Override
            public void onSuccess(CommonRestResponse commonRestResponse) {
                completableFuture.complete(commonRestResponse.statusCode() == 201);
            }

            @Override
            public void onError(Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }
        });

        return completableFuture;
    }

    @Override
    @Async
    public CompletableFuture<List<String>> listBlobs() {
        CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();
        containerURL.listBlobsFlatSegment(null, ListBlobsOptions.DEFAULT, Context.NONE).subscribe(new SingleObserver<ContainerListBlobFlatSegmentResponse>() {
            @Override
            public void onSubscribe(Disposable disposable) {

            }

            @Override
            public void onSuccess(ContainerListBlobFlatSegmentResponse containerListBlobFlatSegmentResponse) {
                List<String> strings = containerListBlobFlatSegmentResponse.body().segment().blobItems().stream().map(BlobItem::name).collect(Collectors.toList());
                completableFuture.complete(strings);
            }

            @Override
            public void onError(Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }
        });

        return completableFuture;
    }

    @Override
    @Async
    public CompletableFuture<File> download(String filename) throws Exception {
        String[] fileNameParts = filename.split("\\.");
        File file = File.createTempFile(fileNameParts[0], fileNameParts[1]);

        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.WRITE);

        BlobURL blobURL = containerURL.createBlobURL(filename);

        CompletableFuture<File> completableFuture = new CompletableFuture<>();
        TransferManager.downloadBlobToFile(fileChannel, blobURL, BlobRange.DEFAULT, TransferManagerDownloadFromBlobOptions.DEFAULT).subscribe(new SingleObserver<BlobDownloadHeaders>() {
            @Override
            public void onSubscribe(Disposable disposable) {
            }

            @Override
            public void onSuccess(BlobDownloadHeaders blobDownloadHeaders) {
                completableFuture.complete(file);
            }

            @Override
            public void onError(Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }
        });

        return completableFuture;
    }


    @Override
    @Async
    public CompletableFuture<Boolean> delete(String filename) {
        BlobURL blobURL = containerURL.createBlobURL(filename);

        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        blobURL.delete(DeleteSnapshotsOptionType.INCLUDE, BlobAccessConditions.NONE, Context.NONE).subscribe(new SingleObserver<BlobDeleteResponse>() {
            @Override
            public void onSubscribe(Disposable disposable) {
            }

            @Override
            public void onSuccess(BlobDeleteResponse blobDeleteResponse) {
                completableFuture.complete(Boolean.TRUE);
            }

            @Override
            public void onError(Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }
        });

        return completableFuture;
    }
}
