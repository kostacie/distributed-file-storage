# Distributed File Storage

## Overview
This project is a distributed file storage system that allows clients to store and retrieve files over a gRPC-based architecture. It consists of three main components:

- **Coordinator**: Handles client requests, manages file metadata, and assigns DataNodes.
- **DataNode**: Stores actual file data.
- **Client**: Provides an interface for users to interact with the file system.

## Features
### _**Important:** not all features are implemented!_
- Supports **file writing and reading** operations.
- Uses **Protobuf & gRPC** for communication between components.
- **Support** for multiple DataNodes.

## System Components
### **Coordinator**
- Receives client requests.
- Stores the mapping of file paths to DataNodes.
- Assigns an available DataNode for file storage.
- Returns the DataNode address and file upload ID.

### **DataNode**
- Stores the actual file data.
- Handles file uploads and downloads.
- Communicates with the Coordinator to register/unregister itself.

### **Client**
- Sends requests to the Coordinator.
- Uploads file data to the assigned DataNode.

## Installation & Setup
### **Prerequisites**
- Java 17
- Maven
- Docker & Docker Compose - **not supported yet** due to errors.

### **Build the Project**
```sh
mvn clean package
```

### **Manual Execution**
#### Start Coordinator
```sh
java -jar coordinator/target/coordinator.jar
```

#### Start DataNodes
```sh
java -jar datanode/target/datanode.jar 5001 datanode1
java -jar datanode/target/datanode.jar 5002 datanode2
java -jar datanode/target/datanode.jar 5003 datanode3
```

#### Start Client
```sh
java -jar client/target/client.jar
```

## Usage Examples
### **Upload a File**
```sh
java -jar client.jar write /path/to/file.txt
```

### **Download a File**
```sh
java -jar client.jar read /path/to/file.txt
```


