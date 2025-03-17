package ru.kostacie;

import coordinator.CoordinatorProto.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Координатор управляет хранением файлов и переправкой запросов.
 */
@Slf4j
@Getter
public class CoordinatorService extends coordinator.CoordinatorGrpc.CoordinatorImplBase {

    // Активные DataNode
    private final Set<String> dataNodes = ConcurrentHashMap.newKeySet();

    // Пути к файлам и DataNode
    private final Map<String, String> fileLocations = new ConcurrentHashMap<>();

    // Файлы и upload_id
    private final Map<String, String> fileUploadIds = new ConcurrentHashMap<>();

    /**
     * Добавляет DataNode в координатор.
     *
     * @param request          Запрос с адресом DataNode.
     * @param responseObserver Ответ клиенту.
     */
    @Override
    public void registerDataNode(RegisterDataNodeRequest request, StreamObserver<RegisterDataNodeResponse> responseObserver) {
        String dataNodeAddress = request.getAddress();
        dataNodes.add(dataNodeAddress);
        log.info("DataNode added: {}", dataNodeAddress);

        RegisterDataNodeResponse response = RegisterDataNodeResponse.newBuilder()
                .setSuccess(true)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Удаляет DataNode из координатора.
     *
     * @param request          Запрос с адресом DataNode.
     * @param responseObserver Ответ клиенту.
     */
    @Override
    public void unregisterDataNode(UnregisterDataNodeRequest request, StreamObserver<UnregisterDataNodeResponse> responseObserver) {
        String dataNodeAddress = request.getAddress();
        dataNodes.remove(dataNodeAddress);
        log.info("DataNode removed: {}", dataNodeAddress);

        UnregisterDataNodeResponse response = UnregisterDataNodeResponse.newBuilder()
                .setSuccess(true)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Обрабатывает запрос на запись файла.
     * Назначает DataNode для хранения файла.
     *
     * @param request          Запрос с путем к файлу.
     * @param responseObserver Ответ клиенту.
     */
    @Override
    public void writeFile(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        try {
            if (dataNodes.isEmpty()) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("No available DataNodes")
                        .asRuntimeException());
                return;
            }

            String filePath = request.getFilePath();
            if (fileLocations.containsKey(filePath)) {
                responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("File already exists: " + filePath)
                        .asRuntimeException());
                return;
            }

            // Выбираем случайный DataNode
            List<String> nodeList = new ArrayList<>(dataNodes);
            String selectedDataNode = nodeList.get(new Random().nextInt(nodeList.size()));

            // Генерируем уникальный upload_id
            String uploadId = UUID.randomUUID().toString();
            fileLocations.put(filePath, selectedDataNode);
            fileUploadIds.put(filePath, uploadId);

            log.info("File '{}' will be written to DataNode '{}' with upload_id '{}'", filePath, selectedDataNode, uploadId);

            WriteFileResponse response = WriteFileResponse.newBuilder()
                    .setDataNodeAddress(selectedDataNode)
                    .setUploadId(uploadId)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error processing writeFile request: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal server error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    /**
     * Обрабатывает запрос на чтение файла.
     *
     * @param request          Запрос с путем к файлу.
     * @param responseObserver Ответ клиенту.
     */
    @Override
    public void readFile(ReadFileRequest request, StreamObserver<ReadFileResponse> responseObserver) {
        try {
            String filePath = request.getFilePath();
            String dataNodeAddress = fileLocations.get(filePath);
            String uploadId = fileUploadIds.get(filePath);

            if (dataNodeAddress == null || uploadId == null) {
                log.error("File '{}' not found", filePath);
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("File not found: " + filePath)
                        .asRuntimeException());
                return;
            }

            if (!dataNodes.contains(dataNodeAddress)) {
                log.error("DataNode '{}' is unavailable", dataNodeAddress);
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("DataNode is unavailable: " + dataNodeAddress)
                        .asRuntimeException());
                return;
            }

            log.info("File '{}' found on DataNode '{}' with upload_id '{}'", filePath, dataNodeAddress, uploadId);

            ReadFileResponse response = ReadFileResponse.newBuilder()
                    .setDataNodeAddress(dataNodeAddress)
                    .setFileId(uploadId)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error during readFile request: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Server error: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
