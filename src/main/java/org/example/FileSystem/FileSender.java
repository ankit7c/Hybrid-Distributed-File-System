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

public class FileSender extends Thread {
    String localFileName;
    // IpAddress and port where file is to be sent.
    String IpAddress;
    int port;
    String result;

    public FileSender(String localFileName, String IpAddress, int port) {
        this.localFileName = localFileName;
        this.IpAddress = IpAddress;
        this.port = port + 20;
    }
    public void sendFile() {
        System.out.println("Connecting to server at " + IpAddress + ":" + port);
        try (SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(IpAddress, port));
             FileChannel fileChannel = FileChannel.open(Paths.get(localFileName), StandardOpenOption.READ)) {

            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();
                socketChannel.write(buffer);
                buffer.clear();
            }
            System.out.println("File sent successfully!");
            result = "File sent successfully!";
        } catch (IOException e) {
            System.out.println("Error sending file: " + e.getMessage());
            result = "File not able to be sent!";
        }
    }

    public void run(){
        sendFile();
    }
}
