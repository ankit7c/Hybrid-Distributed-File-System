package org.example.FileSystem;

import org.example.entities.FDProperties;
import org.example.entities.FileTransferManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileReceiver extends Thread {
    int port;
    String localFileName;

    public FileReceiver() {
        this.port = (int) FDProperties.getFDProperties().get("machinePort") + 20;
    }

    public void run(){
        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(port));
            System.out.println("Server listening on port " + port);
            while (true) {
                try (SocketChannel socketChannel = serverSocketChannel.accept()) {

                    // Step 1: Read the metadata
                    ByteBuffer metadataBuffer = ByteBuffer.allocate(256); // assuming metadata is less than 256 bytes
                    socketChannel.read(metadataBuffer);
                    metadataBuffer.flip();
                    String metadata = new String(metadataBuffer.array(), 0, metadataBuffer.limit());

                    // Parse the metadata to extract filename and size (assuming format "FILENAME:filename;SIZE:size")
                    String[] metadataParts = metadata.split(";");

                    long fileSize = 0;
                    String fileOp = null;
                    String fileType = null;
                    String message = null;
                    for (String part : metadataParts) {
                        if (part.startsWith("FILENAME:")) {
                            localFileName = part.substring("FILENAME:".length());
                        } else if (part.startsWith("SIZE:")) {
                            fileSize = Long.parseLong(part.substring("SIZE:".length()));
                        } else if (part.startsWith("TYPE:")) {
                            fileType = part.substring("TYPE:".length());
                        } else if (part.startsWith("OP:")) {
                            fileOp = part.substring("OP:".length());
                        } else if (part.startsWith("MESSAGE:")) {
                            message = part.substring("MESSAGE:".length());
                        }
                    }
                    System.out.println("Receiving File " + localFileName + " from " + socketChannel.getRemoteAddress());
                    System.out.println(message);

                    if (localFileName == null || fileSize == 0 || fileOp == null || fileType == null) {
                        System.out.println("Invalid metadata received");
                        continue; // Skip to the next connection if metadata is invalid
                    }

                    //Based on File Operation choose the option to create or append
                    FileChannel fileChannel = null;
                    if(fileOp.equals("CREATE")){
                        fileChannel = FileChannel.open(Paths.get(localFileName), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    }else if(fileOp.equals("APPEND")){
                        fileChannel = FileChannel.open(Paths.get(localFileName), StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                    }
                    ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
                    while (socketChannel.read(buffer) > 0) {
                        buffer.flip();
                        fileChannel.write(buffer);
                        buffer.clear();
                    }
                    System.out.println("File received successfully!");
                    System.out.println("File saved at " + localFileName);
                    FileTransferManager.logEvent("File received: " + localFileName);

                } catch (IOException e) {
                    System.out.println("Error during file reception: " + e.getMessage());
                    FileTransferManager.logEvent("File reception failed for " + localFileName + ": " + e.getMessage());
                }
                localFileName = null;

            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error during Server start: " + e.getMessage());
        }
    }

}
