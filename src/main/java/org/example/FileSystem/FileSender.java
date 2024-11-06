package org.example.FileSystem;

import org.example.entities.FileTransferManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileSender extends Thread {
    String localFileName;
    String hyDFSFileName;
    // IpAddress and port where file is to be sent.
    String IpAddress;
    int port;
    String result;
    String sendType;
    String fileOp;
    String message;

    public FileSender(String localFileName,
                      String hyDFSFileName,
                      String IpAddress,
                      int port,
                      String sendType,
                      String fileOp,
                      String message) {
        this.localFileName = localFileName;
        this.hyDFSFileName = hyDFSFileName;
        this.IpAddress = IpAddress;
        this.port = port + 20;
        this.sendType = sendType;
        this.fileOp = fileOp;
        this.message = message;
    }
    public void sendFile() {
        System.out.println("Connecting to server at " + IpAddress + ":" + port);
        try (SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(IpAddress, port));
             FileChannel fileChannel = FileChannel.open(Paths.get(localFileName), StandardOpenOption.READ)) {

            //Prepare metadata and send it
            String metadata = "FILENAME:" + hyDFSFileName + ";SIZE:" + fileChannel.size() + ";TYPE:" + sendType + ";OP:" + fileOp + ";MESSAGE:" + message;
            ByteBuffer metadataBuffer = ByteBuffer.wrap(metadata.getBytes());
            socketChannel.write(metadataBuffer);
            // Ensure the metadata is sent before file transfer
            socketChannel.socket().getOutputStream().flush();

            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();
                socketChannel.write(buffer);
                buffer.clear();
            }
            System.out.println("File sent successfully!");
            result = "File sent successfully!";
            FileTransferManager.logEvent("File sent: " + localFileName);
        } catch (IOException e) {
            System.out.println("Error sending file: " + e.getMessage());
            result = "File not able to be sent!";
            FileTransferManager.logEvent("File sending failed for " + localFileName + ": " + e.getMessage());
        }
    }

//    public void run(){
//        sendFile();
//    }
}
