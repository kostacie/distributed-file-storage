package ru.kostacie;


public class Client
{
    private static final int COORDINATOR_PORT = 50050;
    private static final String COORDINATOR_HOST = "localhost";


    public static void main(String[] args) {

        String operation = args[0];
        String filePath = args[1];
        ClientService client = new ClientService(COORDINATOR_HOST, COORDINATOR_PORT);

        switch (operation) {
            case "upload":
                client.uploadFile(filePath);
                break;
            case "download":
                client.downloadFile(filePath);
                break;
            default:
                System.out.println("Unknown operation. Use 'upload' or 'download'.");
        }
    }
}
