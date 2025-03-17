package ru.kostacie;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class CoordinatorServer
{
    private static final int PORT = 5051;
    private Server server;

    public static void main( String[] args ) throws IOException, InterruptedException {
        CoordinatorServer coordinator = new CoordinatorServer();
        coordinator.startCoordinator();
        coordinator.awaitTermination();
    }

    private void startCoordinator() throws IOException {
        CoordinatorService coordinatorService = new CoordinatorService();
        server = ServerBuilder.forPort(PORT)
                .addService(coordinatorService)
                .build()
                .start();

        log.info("Coordinator started on port {}", PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            log.info("Coordinator stopped");
        }));
    }

    private void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }
}
