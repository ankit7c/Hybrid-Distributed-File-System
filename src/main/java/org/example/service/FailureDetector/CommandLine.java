package org.example.service.FailureDetector;

import org.example.Client;
import org.example.FileSystem.HashFunction;
import org.example.FileSystem.Sender;
import org.example.Server;
import org.example.entities.FDProperties;
import org.example.entities.FileData;
import org.example.entities.Member;
import org.example.entities.MembershipList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import static org.example.entities.FDProperties.fDProperties;

// Comma class that will be executed by multiple threads
/**
 * This Class is handle command line commands
 */
public class CommandLine implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CommandLine.class);
    private ConcurrentHashMap<String, Integer> map;
    private String threadName;


    @Override
    public void run() {
        // Each thread updates the map with its own name as the key
        while(true) {
            System.out.println("Enter The command");
            Scanner scanner = new Scanner(System.in);
            String command = scanner.nextLine();
            String[] list = command.split(" ");
            Dissemination d = new Dissemination();
            Sender sender = new Sender();
            try {
                if (command.startsWith("grep")) {
                    Client c = new Client();
                    c.runClient(command);
                } else if(command.startsWith("drop")) {
                    double dropProb = Double.parseDouble(command.substring(4));
                    fDProperties.put("dropProbability", dropProb);
                } else {
                    switch (list[0]) {
                        case "list_mem":
                            System.out.println("Membership List");
                            MembershipList.printMembersId();
                            break;

                        case "list_self":
                            System.out.println("Node self id");
                            String id = FDProperties.getFDProperties().get("machineIp") + "_" + String.valueOf(FDProperties.getFDProperties().get("machinePort")) + "_"
                                    + FDProperties.getFDProperties().get("versionNo");
                            System.out.println(id);
                            break;
                        case "join":
                            System.out.println("Joining Node");
                            Server s = new Server();
                            s.startServer();
                            break;
                        case "leave":
                            d.sendLeaveMessage();
                            break;

                        case "enable_sus":

                        case "disable_sus":
                            d.sendSwitch();
                            break;

                        case "status_sus":
                            System.out.println(FDProperties.getFDProperties().get("isSuspicionModeOn"));
                            break;

                        // Commands for Distributed File System Handling
                        case "create":
                            sender.uploadFile(list[1], list[2]);
                            System.out.println(list[0] + list[1] + list[2]);
                            break;

                        case "get":
                            sender.get_File(list[1], list[2]);
                            System.out.println(list[0] + list[1] + list[2]);
                            break;
                        case "append":
                            sender.append_File(list[1], list[2]);
                            System.out.println(list[0] + list[1] + list[2]);
                            break;
                        case "merge":
                            break;
                        case "ls":
//                          TODO  below code is wrong ask each machine if they have this file
                            int fileNameHash = HashFunction.hash(list[1]);
                            List<Member> memberslist = new ArrayList<>();
                            memberslist.add(MembershipList.getMemberById(fileNameHash));
                            memberslist.addAll(MembershipList.getNextMembers(fileNameHash));
                            System.out.println("File " + list[1] + " with id " + fileNameHash + " is present at below machines");
                            for (Member member : memberslist) {
                                System.out.println("Member id: " + member.getId());
                            }
                            break;
                        case "store":
//                            list the set of file names (along with their IDs) that are replicated (stored) on HyDFS at this
//                            (local) process/VM. This should NOT include files stored on the local file system.
//                            Also, print the process/VM’s ID on the ring.
                            System.out.println("Stored HyDFS files on the " +
                                    String.valueOf(FDProperties.getFDProperties().get("machineName")) +
                                    " with ring id " + MembershipList.selfId);
                            for(String fileName : FileData.getOwnedFiles()) {
                                System.out.println(fileName);
                            }
                            break;
                        case "getFromReplica":
                            sender.getFileFromReplica(list[1], list[2], list[3]);
                            break;
                        case "list_mem_ids":
//                            augment list_mem from MP2 to also print the ID on the ring which each node in the membership list maps to
                            MembershipList.memberslist.forEach((k, v) -> System.out.println(k + "," + v));
                            break;
                        case "multiappend":
                            String hyDFSFileName = list[1];
                            List<String> VMs = new ArrayList<>();
                            List<String> localFiles = new ArrayList<>();
                            for(int i=2; i<list.length; i++) {
                                if(list[i].contains("Machine")){
                                    VMs.add(list[i]);
                                }else{
                                    localFiles.add(list[i]);
                                }
                            }
                            sender.sendMultiAppendRequests(hyDFSFileName, VMs, localFiles);
                        default:
                            System.out.println("Invalid command");
                            logger.error("Invalid command");

                    }
                }

            } catch (Exception e) {
                logger.error("Error in Commandline while exectuing  command {}  Error  : {}", command, e);
            }
        }
    }
}
