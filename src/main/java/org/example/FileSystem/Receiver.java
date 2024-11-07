package org.example.FileSystem;

import org.example.entities.FDProperties;
import org.example.entities.FileTransferManager;
import org.example.entities.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Receiver extends Thread {

    public void receiveMessage(){
        int serverPort = (int) FDProperties.getFDProperties().get("machinePort") + 10;
        FileTransferManager fileTransferManager = new FileTransferManager();
        try (var serverSocket = new ServerSocket(serverPort)) { // Server listening on port 5000
            System.out.println("Server is listening on port " + serverPort);

            // Accept client connection
            while (true) {
                try (Socket socket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                    System.out.println("Client connected");

                    // Read message from client
                    String received = in.readLine();
                    Message message = Message.process(socket.getInetAddress(), String.valueOf(socket.getPort()), received);
                    System.out.println("Received from client: " + message);
                    String response = "Successful";
                    switch (message.getMessageName()){
//                        case "creating_HDFS_file" :
//                            try {
//                                FileReceiver fileReceiver = new FileReceiver((String) message.getMessageContent().get("hyDFSFileName"), "sending_file");
//                                fileReceiver.start();
//                                out.println(response);
//                                //TODO write a code to create replicas of the received file
//                            }catch (Exception e){
//                                e.printStackTrace();
//                            }
//                            break;
                        case "get_file" :
                            //TODO write code for sending the file which is demanded
                            try {
                                out.println(response);
//                                String receivePort = (String) message.getMessageContent().get("fileReceiverPort");
                                String hyDFSFileName = (String) message.getMessageContent().get("hyDFSFileName");
//                                FileSender fileSender = new FileSender((String) message.getMessageContent().get("hyDFSFileName"), message.getIpAddress().getHostAddress(), Integer.parseInt(receivePort));
                                FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                        hyDFSFileName,
                                        hyDFSFileName,
                                        message.getIpAddress().getHostAddress(),
                                        Integer.parseInt(String.valueOf(message.getMessageContent().get("senderPort"))),
                                        "CREATE",
                                        "CREATE",
                                        "Sending Requested File"
                                ));
//                                fileSender.start();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
                        case "get_file_from_replica" :
                            try {
                                out.println(response);
//                                String receivePort = (String) message.getMessageContent().get("fileReceiverPort");
                                String hyDFSFileName = (String) message.getMessageContent().get("hyDFSFileName");
//                                FileSender fileSender = new FileSender((String) message.getMessageContent().get("hyDFSFileName"), message.getIpAddress().getHostAddress(), Integer.parseInt(receivePort));
                                FileTransferManager.getRequestQueue().addRequest(new FileSender(
                                        hyDFSFileName,
                                        hyDFSFileName,
                                        message.getIpAddress().getHostAddress(),
                                        Integer.parseInt(String.valueOf(message.getMessageContent().get("senderPort"))),
                                        "CREATE",
                                        "CREATE",
                                        "Sending Requested File"
                                ));
//                                fileSender.start();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            break;
//                        case "append_file":
//                            try {
////                                FileReceiver fileReceiver = new FileReceiver((String) message.getMessageContent().get("hyDFSFileName"), "append_file");
////                                fileReceiver.start();
//                                //TODO write a code to append data to the replicas
//                                //We can start a thread which will in backend send the data to the replicas
//                                //After the replicas are updated we can send the result back to the querier.
//                                out.println(response);
//                            }catch (Exception e){
//                                e.printStackTrace();
//                            }
//                            break;
//                        case "receive_replica":
//                            try {
//                                FileReceiver fileReceiver = new FileReceiver((String) message.getMessageContent().get("hyDFSFileName"), "receive_replica");
//                                fileReceiver.start();
//                                out.println(response);
//                                //TODO write a code to create replicas of the received file
//                            }catch (Exception e){
//                                e.printStackTrace();
//                            }
//                            break;
//                        case "append_replica":
//                            try {
//                                FileReceiver fileReceiver = new FileReceiver((String) message.getMessageContent().get("hyDFSFileName"), "append_replica");
//                                fileReceiver.start();
//                                out.println(response);
//                            }catch (Exception e){
//                                e.printStackTrace();
//                            }
//                            break;
//                        case "receive_Multi_append":
//                            break;
                        case "/exit":
                    }
                    // Send response back to client

                    System.out.println("Sent to client: " + response);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void run() {
        receiveMessage();
    }
}
