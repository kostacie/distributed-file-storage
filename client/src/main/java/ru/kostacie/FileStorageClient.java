package ru.kostacie;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class FileStorageClient
{
    public static final int PORT = 50054;
    private final ManagedChannel channel;

    public FileStorageClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, PORT)
                .usePlaintext()
                .build();
    }

    public static void main( String[] args )
    {
        FileStorageClient client = new FileStorageClient("localhost", PORT);
    }
}
