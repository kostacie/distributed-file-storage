package ru.kostacie;


import java.io.IOException;

/**
 * Клиент для работы с операциями чтения и записи файлов.
 */
public class Client
{
    private static final int COORDINATOR_PORT = 5051;
    private static final String COORDINATOR_HOST = "localhost";


    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("For file writing: java Client write -file_path-");
            System.out.println("For file reading: java Client read -file_path-");
            System.exit(1);
        }

        String operation = args[0];
        String filePath = args[1];
        ClientService client = new ClientService(COORDINATOR_HOST, COORDINATOR_PORT);

        switch (operation) {
            case "write":
                client.writeFile(filePath);
                break;
            case "download":
                client.readFile(filePath);
                break;
            default:
                System.out.println("Unknown operation. Use 'write' or 'read'.");
        }
    }
}
