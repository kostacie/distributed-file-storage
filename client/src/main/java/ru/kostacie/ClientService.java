package ru.kostacie;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import coordinator.CoordinatorGrpc;
import coordinator.CoordinatorProto.*;
import datanode.DataNodeGrpc;
import datanode.DataNodeProto.*;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Клиент для взаимодействия с CoordinatorService и DataNode.
 */
@Slf4j
@RequiredArgsConstructor
public class ClientService {
    private final CoordinatorGrpc.CoordinatorBlockingStub coordinatorStub;

    public ClientService(String coordinatorHost, int coordinatorPort) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort)
                .keepAliveTime(60, TimeUnit.SECONDS)
                .usePlaintext()
                .build();
        coordinatorStub = CoordinatorGrpc.newBlockingStub(channel);
    }

    /**
     * Загружает файл в хранилище.
     *
     * @param filePath Путь к файлу.
     */
    public void writeFile(String filePath) throws IOException {
        WriteFileRequest request = WriteFileRequest.newBuilder().setFilePath(filePath).build();
        WriteFileResponse response = coordinatorStub.writeFile(request);

        final CountDownLatch finishLatch = new CountDownLatch(1);


        String[] addressParts = response.getDataNodeAddress().split(":");
        String host = addressParts[0];
        int port = Integer.parseInt(addressParts[1]);

        ManagedChannel dataNodeChannel = ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(60, TimeUnit.SECONDS)
                .usePlaintext()
                .build();
        try {
            DataNodeGrpc.DataNodeStub dataNodeStub = DataNodeGrpc.newStub(dataNodeChannel);

            byte[] fileData = Files.readAllBytes(Paths.get(filePath));
            ByteString content = ByteString.copyFrom(fileData);
            String uploadId = response.getUploadId();
            UploadFileRequest uploadRequest = UploadFileRequest.newBuilder()
                    .setUploadId(uploadId)
                    .setContent(content)
                    .build();

            StreamObserver<UploadFileRequest> requestObserver = dataNodeStub.uploadFile(new StreamObserver<>() {
                @Override
                public void onNext(UploadFileResponse uploadResponse) {
                    log.info("File uploaded successfully");
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Failed uploading: {}", t.getMessage());
                }

                @Override
                public void onCompleted() {
                    log.info("Upload completed.");
                }
            });
            finishLatch.await(5, TimeUnit.SECONDS);

            requestObserver.onNext(uploadRequest);
            requestObserver.onCompleted();
        } catch (InterruptedException e) {
            log.error("Error file uploading: {}", e.getMessage(), e);
            throw new RuntimeException("Error file uploading", e);
        } finally {
            dataNodeChannel.shutdown();
        }
    }

    /**
     * Скачивает файл из хранилища.
     *
     * @param filePath Путь к файлу.
     */
    public void readFile(String filePath) {
        ReadFileRequest request = ReadFileRequest.newBuilder().setFilePath(filePath).build();
        final CountDownLatch finishLatch = new CountDownLatch(1);

        try {
            ReadFileResponse response;
            try {
                response = coordinatorStub.readFile(request);
            } catch (StatusRuntimeException e) {
                log.error("gRPC error during file request: {}", e.getStatus(), e);
                throw new RuntimeException("Failed to download file: " + e.getStatus().getDescription(), e);
            }

            String[] addressParts = response.getDataNodeAddress().split(":");
            String host = addressParts[0];
            int port = Integer.parseInt(addressParts[1]);

            ManagedChannel dataNodeChannel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataNodeGrpc.DataNodeStub dataNodeStub = DataNodeGrpc.newStub(dataNodeChannel);

                DownloadFileRequest downloadRequest = DownloadFileRequest.newBuilder()
                        .setFileId(response.getFileId())
                        .build();

                StreamObserver<DownloadFileResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(DownloadFileResponse responseChunk) {
                        try {
                            byte[] chunkData = responseChunk.getContent().toByteArray();
                            outputStream.write(chunkData);
                            String chunkText = new String(chunkData, StandardCharsets.UTF_8);
                            System.out.println(chunkText);
                            System.out.flush();
                        } catch (IOException e) {
                            log.error("Error reading file: {}", e.getMessage(), e);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Error during file uploading: {}", t.getMessage(), t);
                    }

                    @Override
                    public void onCompleted() {
                        try {
                            byte[] fileData = outputStream.toByteArray();
                            Files.write(Paths.get(filePath), fileData);
                            log.info("File downloaded successfully: {}", filePath);
                        } catch (IOException e) {
                            log.error("Error during saving file: {}", e.getMessage(), e);
                        }
                    }
                };
                dataNodeStub.downloadFile(downloadRequest, responseObserver);
                finishLatch.await(5, TimeUnit.MINUTES);

            } finally {
                dataNodeChannel.shutdown();
                try {
                    if (!dataNodeChannel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        dataNodeChannel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    dataNodeChannel.shutdownNow();
                }
            }
        } catch (Exception e) {
            log.error("Error file downloading: {}", e.getMessage(), e);
            throw new RuntimeException("Error file downloading", e);
        }
    }
}
