package com.innocv.msStorageBlobSpike.controllers;

import com.innocv.msStorageBlobSpike.dtos.FileDto;
import com.innocv.msStorageBlobSpike.services.BlobStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/file")
public class FileController {

    @Autowired
    private BlobStorageService blobStorageService;

    @PostMapping
    @ResponseBody
    public ResponseEntity<String> uploadFile(@RequestBody FileDto fileDto) throws Exception {
        File file = new File(fileDto.getPath());
        CompletableFuture<Boolean> result = blobStorageService.selectContainer("test").upload(file);
        String bodyResult = "File is uploaded: " + result.get();
        return new ResponseEntity<>(bodyResult, HttpStatus.CREATED);
    }

    @GetMapping
    @ResponseBody
    public ResponseEntity<List<String>> listFiles() throws Exception {
        CompletableFuture<List<String>> future = blobStorageService.selectContainer("test").listBlobs();
        return new ResponseEntity<>(future.get(), HttpStatus.OK);
    }

    @GetMapping("/{fileName}")
    @ResponseBody
    public HttpEntity<byte[]> downloadFile(@PathVariable String fileName) throws Exception {
        CompletableFuture<File> future = blobStorageService.selectContainer("test").download(fileName);
        File file = future.get();

        byte[] document = FileCopyUtils.copyToByteArray(file);

        HttpHeaders header = new HttpHeaders();
        header.setContentType(new MediaType("application", "octet-stream"));
        header.set("Content-Disposition", "inline; filename=" + file.getName());
        header.setContentLength(document.length);

        return new HttpEntity<>(document, header);
    }

    @DeleteMapping("/{fileName}")
    @ResponseBody
    public ResponseEntity<String> deleteFile(@PathVariable String fileName) throws Exception {
        CompletableFuture<Boolean> future = blobStorageService.selectContainer("test").delete(fileName);
        String bodyResult = "File is deleted: " + future.get();
        return new ResponseEntity<>(bodyResult, HttpStatus.OK);
    }
}
