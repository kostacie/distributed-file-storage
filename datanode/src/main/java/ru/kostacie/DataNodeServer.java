package ru.kostacie;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class DataNodeServer {

    public static final int PORT = 50051;

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = startServer();
        shutdownServer(server);
        server.awaitTermination();
    }

    private static void shutdownServer(Server server) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down DataNode");
            server.shutdown();
        }));
    }

    private static Server startServer() throws IOException {
        FileStorage fileStorage = new FileStorage();
        DataNodeService dataNodeService = new DataNodeService(fileStorage);

        Server server = ServerBuilder.forPort(PORT)
                .addService(dataNodeService)
                .build()
                .start();

        log.info("DataNode started on port {}", PORT);
        return server;
    }
}
