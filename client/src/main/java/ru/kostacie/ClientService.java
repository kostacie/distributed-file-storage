package ru.kostacie;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import coordinator.CoordinatorGrpc;
import coordinator.CoordinatorProto.*;
import datanode.DataNodeGrpc;
import datanode.DataNodeProto.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Клиент для взаимодействия с CoordinatorService и DataNode.
 */
@Slf4j
public class ClientService {
    private final CoordinatorGrpc.CoordinatorBlockingStub coordinatorStub;

    public ClientService(String coordinatorHost, int coordinatorPort) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort)
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

        String[] addressParts = response.getDataNodeAddress().split(":");
        String host = addressParts[0];
        int port = Integer.parseInt(addressParts[1]);

        ManagedChannel dataNodeChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
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
        requestObserver.onNext(uploadRequest);
        requestObserver.onCompleted();
    }

    /**
     * Скачивает файл из хранилища.
     *
     * @param filePath Путь к файлу.
     */
    public void readFile(String filePath) {
        ReadFileRequest request = ReadFileRequest.newBuilder().setFilePath(filePath).build();

        try {
            ReadFileResponse response = coordinatorStub.readFile(request);
            String[] addressParts = response.getDataNodeAddress().split(":");
            String host = addressParts[0];
            int port = Integer.parseInt(addressParts[1]);

            ManagedChannel dataNodeChannel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            // Создаем стрим для получения всех чанков файла
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DataNodeGrpc.DataNodeBlockingStub dataNodeStub = DataNodeGrpc.newBlockingStub(dataNodeChannel);
                DownloadFileRequest downloadRequest = DownloadFileRequest.newBuilder()
                        .setFileId(response.getFileId())
                        .build();

                // Собираем чанки из каждого ответа
                Iterator<DownloadFileResponse> responses = dataNodeStub.downloadFile(downloadRequest);
                while (responses.hasNext()) {
                    DownloadFileResponse responseChunk = responses.next();
                    outputStream.write(responseChunk.getContent().toByteArray());
                }

                Files.write(Paths.get(filePath), outputStream.toByteArray());
                log.info("File downloaded successfully: {} ", filePath);
            }

        } catch (IOException e) {
            log.error("Failed при записи файла: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Failed during file uploading: {}", e.getMessage());
        }
    }

}
