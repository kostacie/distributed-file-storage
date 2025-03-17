package ru.kostacie;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import datanode.DataNodeProto.*;
import ru.kostacie.exception.FileUploadException;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Сервис для обработки запросов клиента по загрузке и скачиванию файлов в DataNode.
 */
@Slf4j
@RequiredArgsConstructor
public class DataNodeService extends datanode.DataNodeGrpc.DataNodeImplBase {
    private final FileStorage fileStorage;

    /**
     * Загружает файл на DataNode.
     * @param responseObserver ответ для клиента.
     */
    @Override
    public StreamObserver<UploadFileRequest> uploadFile(StreamObserver<UploadFileResponse> responseObserver) {
        return new StreamObserver<>() {
            private String uploadId;
            private List<byte[]> chunks = new ArrayList<>();

            @Override
            public void onNext(UploadFileRequest request) {
                try {
                    if (request.getUploadId().isEmpty()) {
                        responseObserver.onError(new FileUploadException("Upload ID must not be empty"));
                        return;
                    }
                    // Если файл уже существует - ошибка
                    if (uploadId == null) {
                        uploadId = request.getUploadId();
                        if (fileStorage.fileExists(uploadId)) {
                            responseObserver.onError(new FileUploadException("File already exists"));
                            return;
                        }
                    }
                    // Добавляем каждый чанк в список со всеми данными файла
                    chunks.add(request.getContent().toByteArray());
                } catch (Exception e) {
                    responseObserver.onError(new FileUploadException("File upload failed: " + e.getMessage()));
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error during file uploading: {}", t.getMessage());
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                if (uploadId == null) {
                    responseObserver.onError(new FileUploadException("Upload ID is not set"));
                    return;
                }
                try {
                    // Объединяем чанки
                    byte[] concatedChunks = concatChunks(chunks);
                    // Сохраняем файл в FileStorage
                    fileStorage.saveFile(uploadId, concatedChunks);

                    log.info("File uploading completed. ID: {}", uploadId);

                    UploadFileResponse response = UploadFileResponse.newBuilder().setSuccess(true).build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (IOException e) {
                    responseObserver.onError(new FileUploadException("Failed to write file: " + e.getMessage()));
                }
            }
        };
    }

    /**
     * Читает файл из FileStorage.
     *
     * @param request           запрос с ID файла.
     * @param responseObserver  ответ для клиента.
     */
    @Override
    public void downloadFile(DownloadFileRequest request, StreamObserver<DownloadFileResponse> responseObserver) {
        try {
            String fileId = request.getFileId();
            byte[] fileData = fileStorage.getFile(fileId);
            // Если файла не существует - ошибка
            if (fileData == null) {
                responseObserver.onError(new FileNotFoundException("File not found: " + fileId));
                return;
            }

            ByteString byteContent = ByteString.copyFrom(fileData);
            DownloadFileResponse response = DownloadFileResponse.newBuilder().setContent(byteContent).build();
            responseObserver.onNext(response);

            log.info("File sent: {}", fileId);

            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(new IOException("Downloading is failed: " + e.getMessage()));
        }
    }

    private byte[] concatChunks(List<byte[]> chunks) throws IOException {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            for (byte[] chunk : chunks) {
                byteStream.write(chunk);
            }
            return byteStream.toByteArray();
        }
    }
}
