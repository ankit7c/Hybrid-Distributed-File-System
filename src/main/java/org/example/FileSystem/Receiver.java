package org.example.FileSystem;

import org.example.entities.FDProperties;
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
                        case "sending_file" :
                            FileReceiver fileReceiver = new FileReceiver((String) message.getMessageContent().get("hyDFSFileName"));
                            fileReceiver.start();
                            out.println(response);
                            break;
                        case "get_file" :
                            //TODO write code for sending the file which is demanded
                            out.println(response);
                            String receivePort = (String) message.getMessageContent().get("fileReceiverPort");
                            FileSender fileSender = new FileSender((String) message.getMessageContent().get("hyDFSFileName"), message.getIpAddress().getHostAddress() , Integer.parseInt(receivePort));
                            fileSender.start();
                            break;
                        case "co_ord_get_file":

                            break;
                        case "co_ord_write_file":

                            break;
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
