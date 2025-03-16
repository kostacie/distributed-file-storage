package ru.kostacie;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class CoordinatorServer
{
    public static final int PORT = 50050;

    public static void main( String[] args ) throws IOException, InterruptedException {
        Server server = startServer();
        shutdownServer(server);
        server.awaitTermination();
    }

    private static void shutdownServer(Server server) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down CoordinatorService");
            server.shutdown();
        }));
    }

    private static Server startServer() throws IOException {
        CoordinatorService coordinatorService = new CoordinatorService();
        Server server = ServerBuilder.forPort(PORT)
                .addService(coordinatorService)
                .build()
                .start();

        log.info("Coordinator started on port {}", PORT);
        return server;
    }
}
