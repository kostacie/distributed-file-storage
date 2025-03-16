package ru.kostacie;

import coordinator.CoordinatorProto.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import ru.kostacie.exception.DataNodeNotAvailableException;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Координатор управляет хранением файлов и маршрутизацией запросов.
 */
@Slf4j
public class CoordinatorService extends coordinator.CoordinatorGrpc.CoordinatorImplBase {

    private final Set<String> dataNodes = ConcurrentHashMap.newKeySet(); // Список активных DataNode
    private final Map<String, String> fileLocations = new ConcurrentHashMap<>(); // filePath -> DataNode
    private final Map<String, String> fileUploadIds = new ConcurrentHashMap<>(); // filePath -> upload_id

    /**
     * Регистрация DataNode.
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
     * Удаление DataNode.
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
     * Запись файла (выбор DataNode).
     */
    @Override
    public void writeFile(WriteFileRequest request, StreamObserver<WriteFileResponse> responseObserver) {
        if (dataNodes.isEmpty()) {
            responseObserver.onError(new DataNodeNotAvailableException("No available DataNode"));
            return;
        }

        String filePath = request.getFilePath();
        if (fileLocations.containsKey(filePath)) {
            responseObserver.onError(new RuntimeException("File already exists"));
            return;
        }

        // Выбираем случайную DataNode
        List<String> nodeList = new ArrayList<>(dataNodes);
        String selectedDataNode = nodeList.get(new Random().nextInt(nodeList.size()));

        // Генерируем уникальный file_id
        String uploadId = UUID.randomUUID().toString();
        fileLocations.put(filePath, selectedDataNode);
        fileUploadIds.put(filePath, uploadId);

        log.info("File {} will be written to {} with upload_id {}", filePath, selectedDataNode, uploadId);

        WriteFileResponse response = WriteFileResponse.newBuilder()
                .setDataNodeAddress(selectedDataNode)
                .setUploadId(uploadId)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Чтение файла (поиск DataNode).
     */
    @Override
    public void readFile(ReadFileRequest request, StreamObserver<ReadFileResponse> responseObserver) {
        String filePath = request.getFilePath();
        String dataNodeAddress = fileLocations.get(filePath);
        String uploadId = fileUploadIds.get(filePath);

        if (dataNodeAddress == null || uploadId == null) {
            log.error("File {} is not found", filePath);
            responseObserver.onError(new FileNotFoundException("File is not found"));
            return;
        }

        if (!dataNodes.contains(dataNodeAddress)) {
            log.error("DataNode {} not available", dataNodeAddress);
            responseObserver.onError(new DataNodeNotAvailableException("DataNode is not available"));
            return;
        }

        log.info("File {} was found at address {} with upload_id {}", filePath, dataNodeAddress, uploadId);

        ReadFileResponse response = ReadFileResponse.newBuilder()
                .setDataNodeAddress(dataNodeAddress)
                .setFileId(uploadId)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
