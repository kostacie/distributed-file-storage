package ru.kostacie;

import com.google.protobuf.ByteString;
import datanode.DataNodeProto.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.kostacie.exception.FileUploadException;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Тесты для DataNodeService.
 */
@ExtendWith(MockitoExtension.class)
class DataNodeServiceTest {

    private DataNodeService dataNodeService;

    @Mock
    private FileStorage fileStorage;

    @Mock
    private StreamObserver<UploadFileResponse> uploadResponseObserver;

    @Mock
    private StreamObserver<DownloadFileResponse> downloadResponseObserver;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        dataNodeService = new DataNodeService(fileStorage);
    }

    /**
     * Тест успешной загрузки файла.
     */
    @Test
    void uploadFile_uploadSuccessfully() throws IOException {
        when(fileStorage.fileExists("file123")).thenReturn(false);

        StreamObserver<UploadFileRequest> requestObserver = dataNodeService.uploadFile(uploadResponseObserver);

        requestObserver.onNext(UploadFileRequest.newBuilder()
                .setUploadId("file123")
                .setContent(ByteString.copyFromUtf8("test data"))
                .build());

        requestObserver.onCompleted();

        verify(fileStorage).saveFile(eq("file123"), any(byte[].class));

        ArgumentCaptor<UploadFileResponse> captor = ArgumentCaptor.forClass(UploadFileResponse.class);
        verify(uploadResponseObserver).onNext(captor.capture());
        assertTrue(captor.getValue().getSuccess());

        verify(uploadResponseObserver).onCompleted();
    }

    /**
     * Тест ошибки при загрузке, если uploadId пустой.
     */
    @Test
    void uploadFile_failIfUploadIdEmpty() {
        StreamObserver<UploadFileRequest> requestObserver = dataNodeService.uploadFile(uploadResponseObserver);

        requestObserver.onNext(UploadFileRequest.newBuilder()
                .setUploadId("")
                .setContent(ByteString.copyFromUtf8("test data"))
                .build());

        verify(uploadResponseObserver).onError(any(FileUploadException.class));
    }

    /**
     * Тест ошибки при записи файла, если файл уже существует.
     */
    @Test
    void uploadFile_failIfFileAlreadyExists() throws IOException {
        when(fileStorage.fileExists("file123")).thenReturn(true);

        StreamObserver<UploadFileRequest> requestObserver = dataNodeService.uploadFile(uploadResponseObserver);

        requestObserver.onNext(UploadFileRequest.newBuilder()
                .setUploadId("file123")
                .setContent(ByteString.copyFromUtf8("test data"))
                .build());

        verify(uploadResponseObserver).onError(any(FileUploadException.class));
    }

    /**
     * Тест успешного чтения файла.
     */
    @Test
    void downloadFile_returnFileContent() throws IOException {
        byte[] fileData = "test data".getBytes();
        when(fileStorage.getFile("file123")).thenReturn(fileData);

        DownloadFileRequest request = DownloadFileRequest.newBuilder()
                .setFileId("file123")
                .build();

        dataNodeService.downloadFile(request, downloadResponseObserver);

        ArgumentCaptor<DownloadFileResponse> captor = ArgumentCaptor.forClass(DownloadFileResponse.class);
        verify(downloadResponseObserver).onNext(captor.capture());

        assertEquals(ByteString.copyFrom(fileData), captor.getValue().getContent());

        verify(downloadResponseObserver).onCompleted();
    }

    /**
     * Тест ошибки при чтении файла, если файла нет.
     */
    @Test
    void downloadFile_failIfFileNotFound() throws IOException {
        when(fileStorage.getFile("file123")).thenThrow(new FileNotFoundException("File not found"));

        DownloadFileRequest request = DownloadFileRequest.newBuilder()
                .setFileId("file123")
                .build();

        dataNodeService.downloadFile(request, downloadResponseObserver);

        verify(downloadResponseObserver).onError(argThat(error ->
                error instanceof IOException &&
                        error.getMessage().contains("Downloading failed: File not found")
        ));
    }
}
