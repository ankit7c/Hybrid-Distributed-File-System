package org.example.FileSystem;

import org.example.entities.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class FileReceiver extends Thread {
    int port;
    String hyDFSFileName;

    int tempCounter;

    public FileReceiver() {
        this.port = (int) FDProperties.getFDProperties().get("machinePort") + 20;
        tempCounter = 0;
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
                    int senderId = -1;
                    for (String part : metadataParts) {
                        if (part.startsWith("FILENAME:")) {
                            hyDFSFileName = part.substring("FILENAME:".length());
                        } else if (part.startsWith("SIZE:")) {
                            fileSize = Long.parseLong(part.substring("SIZE:".length()));
                        } else if (part.startsWith("TYPE:")) {
                            fileType = part.substring("TYPE:".length());
                        } else if (part.startsWith("OP:")) {
                            fileOp = part.substring("OP:".length());
                        } else if (part.startsWith("MESSAGE:")) {
                            message = part.substring("MESSAGE:".length());
                        } else if (part.startsWith("SENDERID:")) {
                            senderId = Integer.parseInt(part.substring("SENDERID:".length()));
                        }
                    }
                    
                    System.out.println("Receiving File " + hyDFSFileName + " from " + socketChannel.getRemoteAddress());
                    System.out.println(message);

                    if (hyDFSFileName == null || fileSize == 0 || fileOp == null || fileType == null) {
                        System.out.println("Invalid metadata received");
                        continue; // Skip to the next connection if metadata is invalid
                    }

                    //Based on File Operation choose the option to create or append
                    FileChannel fileChannel = null;
                    FileChannel tempFileChannel = null;
                    if(fileType.equals("UPLOAD")) {
                        if (fileOp.equals("CREATE")) {
                            fileChannel = FileChannel.open(Paths.get(hyDFSFileName), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                        } else if (fileOp.equals("APPEND")) {
                            fileChannel = FileChannel.open(Paths.get(hyDFSFileName), StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                            tempFileChannel = FileChannel.open(Paths.get(hyDFSFileName + tempCounter), StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                            tempCounter++;
                        }
                    }else if(fileType.equals("GET")) {
                        fileChannel = FileChannel.open(Paths.get("local/" + hyDFSFileName), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    }
                    ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
                    while (socketChannel.read(buffer) > 0) {
                        buffer.flip();
                        fileChannel.write(buffer);
                        tempFileChannel.write(buffer);
                        buffer.clear();
                    }
                    System.out.println("File received successfully! and saved at " + hyDFSFileName);

                    if(fileType.equals("UPLOAD")) {
                        if(fileOp.equals("APPEND")) {
                            StandardOpenOption option = StandardOpenOption.APPEND;
                            //get next 2 nodes and send them the request
                            List<Member> members = MembershipList.getNextMembers(HashFunction.hash(hyDFSFileName));
                            for (Member member : members) {
                                FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                        hyDFSFileName+tempCounter,
                                        hyDFSFileName,
                                        member.getIpAddress(),
                                        Integer.parseInt(member.getPort()),
                                        "REPLICA",
                                        "APPEND",
                                        ""));
                            }
                            tempCounter++;
                        }else{
                            FileData.addOwnedFile(hyDFSFileName);
                            //et next 2 nodes and send them the request
                            List<Member> members = MembershipList.getNextMembers(HashFunction.hash(hyDFSFileName));
                            for (Member member : members) {
                                FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                        hyDFSFileName+tempCounter,
                                        hyDFSFileName,
                                        member.getIpAddress(),
                                        Integer.parseInt(member.getPort()),
                                        "REPLICA",
                                        "CREATE",
                                        ""));
                            }
                        }
                    }else if(fileType.equals("REPLICA")) {
                        if (fileOp.equals("CREATE")) {
                            FileData.addReplica(hyDFSFileName, senderId);
                        }
                    }
                    FileTransferManager.logEvent("File received: " + hyDFSFileName);

                } catch (IOException e) {
                    System.out.println("Error during file reception: " + e.getMessage());
                    FileTransferManager.logEvent("File reception failed for " + hyDFSFileName + ": " + e.getMessage());
                }
                hyDFSFileName = null;

            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error during Server start: " + e.getMessage());
        }
    }

}
