package ru.kostacie;

import coordinator.CoordinatorGrpc;
import coordinator.CoordinatorProto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Сервер для работы с DataNode.
 */
@Slf4j
public class DataNodeServer {
    private static final int COORDINATOR_PORT = 5051;
    private static final String COORDINATOR_HOST = "localhost";

    private final int port;
    private final String dataNodeId;
    private Server server;

    public DataNodeServer(int port, String dataNodeId) {
        this.port = port;
        this.dataNodeId = dataNodeId;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Use: java DataNodeServer -port- -dataNodeId-");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String dataNodeId = args[1];

        DataNodeServer dataNode = new DataNodeServer(port, dataNodeId);
        dataNode.startDataNode();
        dataNode.awaitTermination();
    }

    /**
     * Запускает DataNode и добавляет его в CoordinatorService.
     *
     * @throws IOException если не удается запустить сервер.
     */
    public void startDataNode() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new DataNodeService(new FileStorage()))
                .build()
                .start();

        log.info("DataNode {} started on port {}", dataNodeId, port);

        // Регистрируем DataNode в Координаторе
        requestToCoordinator();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            removeFromCoordinator();
            server.shutdown();
            log.info("DataNode {} stopped", dataNodeId);
        }));
    }

    /**
     * Добавляет DataNode в список CoordinatorService.
     */
    private void requestToCoordinator() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(COORDINATOR_HOST, COORDINATOR_PORT)
                .usePlaintext()
                .build();
        CoordinatorGrpc.CoordinatorBlockingStub stub = CoordinatorGrpc.newBlockingStub(channel);

        RegisterDataNodeRequest request = RegisterDataNodeRequest.newBuilder()
                .setAddress(String.format("%s:%d", COORDINATOR_HOST, port))
                .build();

        RegisterDataNodeResponse response = stub.registerDataNode(request);
        log.info("DataNode {} added to Coordinator: {}", dataNodeId, response.getSuccess());
    }

    /**
     * Удаляет DataNode из списка CoordinatorService.
     */
    private void removeFromCoordinator() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(COORDINATOR_HOST, COORDINATOR_PORT)
                .usePlaintext()
                .build();
        CoordinatorGrpc.CoordinatorBlockingStub stub = CoordinatorGrpc.newBlockingStub(channel);

        UnregisterDataNodeRequest request = UnregisterDataNodeRequest.newBuilder()
                .setAddress(String.format("%s:%d", COORDINATOR_HOST, port))
                .build();

        UnregisterDataNodeResponse response = stub.unregisterDataNode(request);
        log.info("DataNode {} removed from Coordinator: {}", dataNodeId, response.getSuccess());
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }
}
