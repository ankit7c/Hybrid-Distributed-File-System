package org.example.FileSystem;

import org.example.entities.FDProperties;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileReceiver  extends Thread {
    String localFileName;
    int port;
    String result;

    public FileReceiver(String localFileName) {
        this.localFileName = localFileName;
        this.port = (int) FDProperties.getFDProperties().get("machinePort") + 20;
    }

    public void receiveFile() {
        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(port));
            System.out.println("Server listening on port " + port);
            try (SocketChannel socketChannel = serverSocketChannel.accept();
                 FileChannel fileChannel = FileChannel.open(Paths.get(localFileName), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
                while (socketChannel.read(buffer) > 0) {
                    buffer.flip();
                    fileChannel.write(buffer);
                    buffer.clear();
                }
                System.out.println("File received successfully!");
                System.out.println("File saved at " + localFileName);
                result = "File received successfully!";

            } catch (IOException e) {
                System.out.println("Error during file reception: " + e.getMessage());
                result = "File reception failed!";
            }
        } catch (IOException e) {
            e.printStackTrace();
            result = "Server setup failed!";
        }
    }

    public void run() {
        receiveFile();
    }
}
