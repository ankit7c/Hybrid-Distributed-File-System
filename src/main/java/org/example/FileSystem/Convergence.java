package org.example.FileSystem;

import org.example.entities.FileData;
import org.example.entities.MembershipList;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Convergence extends Thread {

    public void run() {
        while (true) {
            try {
                Sender s = new Sender();
                List<Integer> sortedKeys = new CopyOnWriteArrayList<>(MembershipList.memberslist.keySet());

                for (int key : MembershipList.failedNodes) {
                    MembershipList.RemoveFromMembersList(key);
                }
                List<String> ownedFiles = FileData.getOwnedFiles();
                for (Integer failedNodeId : MembershipList.failedNodes) {

                    String status = checkPredecessorOrSuccessor(failedNodeId, MembershipList.selfId,sortedKeys);
                    switch (status) {
                        case "Successor1":
                            s.updateReplicas(ownedFiles);
                            break;
                        case "Successor2":
                            s.updateReplicas(ownedFiles);
                            break;

                        case "Predecessor":
                            List<String> replicaFilesOfFailedNode = FileData.getAndRemoveReplicasOfANode(failedNodeId);
                            FileData.addOwnedFiles(replicaFilesOfFailedNode);
                            List<String> updatedownedFiles = FileData.getOwnedFiles();
                            s.updateReplicas(updatedownedFiles);
                            break;
                        default:
                            break;
                    }
                }
                MembershipList.failedNodes.clear();
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }



    }


    private String checkPredecessorOrSuccessor(int target, int current, List<Integer>sortedKeys) {
        int size = sortedKeys.size();
        int currentIndex = sortedKeys.indexOf(current);


        if (currentIndex == -1) {
            return "NotFound";
        }


        int successor1Index = (currentIndex + 1) % size;
        int successor2Index = (currentIndex + 2) % size;
        int predecessorIndex = (currentIndex - 1 + size) % size;

        if (sortedKeys.get(successor1Index).equals(target)) {
            return "Successor1";
        }


        if (sortedKeys.get(successor2Index).equals(target)) {
            return "Successor2";
        }

        if (sortedKeys.get(predecessorIndex).equals(target)) {
            return "Predecessor";
        }
        return "NotFound";
    }
}
