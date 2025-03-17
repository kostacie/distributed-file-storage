package ru.kostacie;

import coordinator.CoordinatorProto.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Тесты для CoordinatorService.
 */
@ExtendWith(MockitoExtension.class)
public class CoordinatorServiceTest {

    @InjectMocks
    private CoordinatorService coordinatorService;

    @Mock
    private StreamObserver<RegisterDataNodeResponse> registerResponseObserver;

    @Mock
    private StreamObserver<UnregisterDataNodeResponse> unregisterResponseObserver;

    @Mock
    private StreamObserver<WriteFileResponse> writeFileResponseObserver;

    @Mock
    private StreamObserver<ReadFileResponse> readFileResponseObserver;

    /**
     * Тест добавления DataNode.
     */
    @Test
    void registerDataNode_addDataNode() {
        RegisterDataNodeRequest request = RegisterDataNodeRequest.newBuilder()
                .setAddress("localhost:5001")
                .build();

        coordinatorService.registerDataNode(request, registerResponseObserver);

        assertTrue(coordinatorService.getDataNodes().contains("localhost:5001"));

        ArgumentCaptor<RegisterDataNodeResponse> captor = ArgumentCaptor.forClass(RegisterDataNodeResponse.class);
        verify(registerResponseObserver).onNext(captor.capture());
        assertTrue(captor.getValue().getSuccess());

        verify(registerResponseObserver).onCompleted();
    }

    /**
     * Тест удаления DataNode.
     */
    @Test
    void unregisterDataNode_removeDataNode() {
        coordinatorService.getDataNodes().add("localhost:5001");

        UnregisterDataNodeRequest request = UnregisterDataNodeRequest.newBuilder()
                .setAddress("localhost:5001")
                .build();

        coordinatorService.unregisterDataNode(request, unregisterResponseObserver);

        assertFalse(coordinatorService.getDataNodes().contains("localhost:5001"));

        ArgumentCaptor<UnregisterDataNodeResponse> captor = ArgumentCaptor.forClass(UnregisterDataNodeResponse.class);
        verify(unregisterResponseObserver).onNext(captor.capture());
        assertTrue(captor.getValue().getSuccess());

        verify(unregisterResponseObserver).onCompleted();
    }

    /**
     * Тест записи файла - успех.
     */
    @Test
    void writeFile() {
        coordinatorService.getDataNodes().add("localhost:5001");

        WriteFileRequest request = WriteFileRequest.newBuilder()
                .setFilePath("file1.txt")
                .build();

        coordinatorService.writeFile(request, writeFileResponseObserver);

        boolean actual = coordinatorService.getFileLocations().containsKey("file1.txt");
        assertTrue(actual);

        ArgumentCaptor<WriteFileResponse> captor = ArgumentCaptor.forClass(WriteFileResponse.class);
        verify(writeFileResponseObserver).onNext(captor.capture());

        assertNotNull(captor.getValue().getUploadId());
        assertEquals("localhost:5001", captor.getValue().getDataNodeAddress());

        verify(writeFileResponseObserver).onCompleted();
    }

    /**
     * Тест записи файла, если DataNode отсутствует.
     */
    @Test
    void writeFile_failIfNoDataNodeAvailable() {
        WriteFileRequest request = WriteFileRequest.newBuilder()
                .setFilePath("file1.txt")
                .build();

        coordinatorService.writeFile(request, writeFileResponseObserver);

        verify(writeFileResponseObserver).onError(any());
    }

    /**
     * Тест чтения файла - успех.
     */
    @Test
    void readFile_successReturnFileLocation() {
        coordinatorService.getFileLocations().put("file1.txt", "localhost:5001");
        coordinatorService.getFileUploadIds().put("file1.txt", "upload123");
        coordinatorService.getDataNodes().add("localhost:5001");

        ReadFileRequest request = ReadFileRequest.newBuilder()
                .setFilePath("file1.txt")
                .build();

        coordinatorService.readFile(request, readFileResponseObserver);

        ArgumentCaptor<ReadFileResponse> captor = ArgumentCaptor.forClass(ReadFileResponse.class);
        verify(readFileResponseObserver).onNext(captor.capture());

        assertEquals("localhost:5001", captor.getValue().getDataNodeAddress());
        assertEquals("upload123", captor.getValue().getFileId());

        verify(readFileResponseObserver).onCompleted();
    }

    /**
     * Тест чтения файла, если файл не найден.
     */
    @Test
    void readFile_failIfFileNotFound() {
        ReadFileRequest request = ReadFileRequest.newBuilder()
                .setFilePath("some.txt")
                .build();

        coordinatorService.readFile(request, readFileResponseObserver);

        verify(readFileResponseObserver).onError(any());
    }
}
