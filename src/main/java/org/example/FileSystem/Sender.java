package org.example.FileSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.entities.*;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sender {
    private ObjectMapper objectMapper;

    public Sender() {
        objectMapper = new ObjectMapper();
    }

    public void temp() {
        try (Socket socket = new Socket("localhost", 5000); // Connect to server on localhost and port 5000
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send message to server
            String message = "Hello from the client!";
            out.println(message);
            System.out.println("Sent to server: " + message);

            // Receive response from server
            String response = in.readLine();
            System.out.println("Received from server: " + response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String sendMessage(String IpAddress, int port, Message message) {
        try (Socket socket = new Socket(IpAddress, port+10);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            System.out.println(port+10);
            String msg = objectMapper.writeValueAsString(message.getMessageContent());
            out.println(msg);
            System.out.println("Sent to server: " + msg);

            String response = in.readLine();
            System.out.println("Received from server: " + response);
            return response;
        } catch (IOException e) {
            e.printStackTrace();
            return "Unsuccessful";
        }
    }

    public String sendFile(String IpAddress, int port, String localFileName, String hyDFSFileName) {
        try {
//            int fileReceiverPort = port;
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", "creating_HDFS_file");
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            messageContent.put("localFileName", localFileName);
            messageContent.put("hyDFSFileName", hyDFSFileName);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message("sending_file",
                    (String) FDProperties.getFDProperties().get("machineIp"),
                    senderPort,
                    messageContent);
            String response = sendMessage(IpAddress, port, msg);
            //TODO send the file to the server
            if (response.equals("Successful")) {
                System.out.println(response);
                FileSender fileSender = new FileSender(localFileName, IpAddress, port);
                fileSender.start();
            }
            return response;
        }catch (Exception e){
            e.printStackTrace();
            return "Unsuccessful";
        }
    }

    public String receiveFile(String IpAddress, int port, String localFileName, String hyDFSFileName, int fileReceiverPort) {
        try {
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", "get_file");
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("fileReceiverPort", String.valueOf(fileReceiverPort));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            messageContent.put("localFileName", localFileName);
            messageContent.put("hyDFSFileName", hyDFSFileName);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message("sending_file",
                    (String) FDProperties.getFDProperties().get("machineIp"),
                    senderPort,
                    messageContent);
            String response = sendMessage(IpAddress, port, msg);
            if (response.equals("Successful")) {
                System.out.println(response);
                FileReceiver fileReceiver = new FileReceiver(localFileName, "receive");
                fileReceiver.start();
            }
            return response;
        }catch (Exception e){
            e.printStackTrace();
            return "Unsuccessful";
        }
    }

    public String appendFile(String IpAddress, int port, String localFileName, String hyDFSFileName) {
        try {
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", "append_file");
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            messageContent.put("localFileName", localFileName);
            messageContent.put("hyDFSFileName", hyDFSFileName);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message("sending_file",
                    (String) FDProperties.getFDProperties().get("machineIp"),
                    senderPort,
                    messageContent);
            String response = sendMessage(IpAddress, port, msg);
            //TODO send the file to the server
            if (response.equals("Successful")) {
                System.out.println(response);
                FileSender fileSender = new FileSender(localFileName, IpAddress, port);
                fileSender.start();
            }
            return response;
        }catch (Exception e){
            e.printStackTrace();
            return "Unsuccessful";
        }
    }

    //TODO send request to receive a file
    //TODO send a request to upload a file
    public String uploadFile(String localFileName, String hyDFSFileName) throws IOException {
        //TODO check if file present in local
        try {
//            System.out.println(localFileName + " " + hyDFSFileName);
//            int fileNameHash = HashFunction.hash(hyDFSFileName);
//            // Get Member who will store the files
//            List<Member> list = new ArrayList<>();
//            list.add(MembershipList.getMemberById(fileNameHash));
////            System.out.println(member.toString());
//            // Get Members where you want to create replicas
//            list.addAll(MembershipList.getNextMembers(fileNameHash));
//            System.out.println(list.size());
//            for (Member member : list) {
//                System.out.println(member.getName());
//                //TODO send a message to the server and make them ready to accept the file
//                String IpAddress = member.getIpAddress();
//                String port = member.getPort();
//                String result = sendFile(IpAddress, Integer.parseInt(port), localFileName, hyDFSFileName);
////                System.out.println("File sent was " + result);
//            }
            int fileNameHash = HashFunction.hash(hyDFSFileName);
            Member member = MembershipList.getMemberById(fileNameHash);
            FileSender fileSender = new FileSender(localFileName, hyDFSFileName, member.getIpAddress(), member.getPort(), "CREATE", "CREATE", "", ma)
        }catch (Exception e){
            e.printStackTrace();
            return "Unable to upload file";
        }
        return "File uploaded successfully";
    }

    public String get_File(String hyDFSFileName, String localFileName) {
        try {
            //TODO need to handle failures
            //If any node is not there then ask for next node
            //If file not present then return File not found, for that need to check in all replicas
            //This request should go by default to Co-ordinator and then it would return the server name with the filepath and then request the file.

            int fileNameHash = HashFunction.hash(hyDFSFileName);
            Member member = MembershipList.getMemberById(fileNameHash);
            Map<String, Object> messageContent = new HashMap<>();
            String IpAddress = member.getIpAddress();
            String port = member.getPort();
            int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
            String result = receiveFile(IpAddress, Integer.parseInt(port), localFileName, hyDFSFileName, fileReceiverPort);
            System.out.println("File receive was " + result);
            return result;
        }catch (Exception e){
            e.printStackTrace();
            return "Unable to receive file";
        }
    }

    //TODO send a request to append a file
    public String append_File(String localFileName, String hyDFSFileName){
        int fileNameHash = HashFunction.hash(hyDFSFileName);
        Member member = MembershipList.getMemberById(fileNameHash);
        Map<String, Object> messageContent = new HashMap<>();
        String IpAddress = member.getIpAddress();
        String port = member.getPort();
//        int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
        String result = appendFile(IpAddress, Integer.parseInt(port), localFileName, hyDFSFileName);
        System.out.println("File receive was " + result);
        return result;
    }

    //TODO get a replica from a node
    public String getFileFromReplica(String VMName, String hyDFSFileName, String localFileName) {
        int hash = HashFunction.hash(VMName);
        Member member = MembershipList.getMemberById(hash);
        String IpAddress = member.getIpAddress();
        int port = Integer.parseInt(member.getPort());
        int fileReceiverPort = (int) FDProperties.getFDProperties().get("machinePort");
        try {
            Map<String, Object> messageContent = new HashMap<>();
            messageContent.put("messageName", "get_file_from_replica");
            messageContent.put("senderName", FDProperties.getFDProperties().get("machineName"));
            messageContent.put("senderIp", FDProperties.getFDProperties().get("machineIp"));
            messageContent.put("senderPort", String.valueOf(FDProperties.getFDProperties().get("machinePort")));
            messageContent.put("fileReceiverPort", String.valueOf(fileReceiverPort));
            messageContent.put("msgId", FDProperties.generateRandomMessageId());
            messageContent.put("localFileName", localFileName);
            messageContent.put("hyDFSFileName", hyDFSFileName);
            String senderPort = "" + FDProperties.getFDProperties().get("machinePort");
            Message msg = new Message("sending_file",
                    (String) FDProperties.getFDProperties().get("machineIp"),
                    senderPort,
                    messageContent);
            String response = sendMessage(IpAddress, port, msg);
            if (response.equals("Successful")) {
                System.out.println(response);
                FileReceiver fileReceiver = new FileReceiver(localFileName, "receive");
                fileReceiver.start();
            }
            return response;
        }catch (Exception e){
            e.printStackTrace();
            return "Unsuccessful";
        }
    }
    //TODO send a request to merge files

    //TODO write a code to send multi appends.

}
