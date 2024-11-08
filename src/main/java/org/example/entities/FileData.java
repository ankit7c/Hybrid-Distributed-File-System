package org.example.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class FileData {
    public static List<String> ownedFiles = new CopyOnWriteArrayList<>();
    public static ConcurrentSkipListMap<String,Integer> replicaMap = new ConcurrentSkipListMap<>();

    public static List<String> getOwnedFiles() {
        return ownedFiles;
    }

    public static void addOwnedFile(String ownedFile) {
        FileData.ownedFiles.add(ownedFile);
    }

    public static ConcurrentSkipListMap<String, Integer> getReplicaMap() {
        return replicaMap;
    }

    public static void addReplica(String fileName, int ownerId) {
        replicaMap.put(fileName, ownerId);
    }

    public static boolean checkFilePresent(String fileName) {
        return replicaMap.containsKey(fileName) || ownedFiles.contains(fileName);
    }

//    public static boolean isFileReplica(String fileName) {
//        return replicaMap.containsKey(fileName);
//    }

    public static List<String> getAndRemoveReplicasOfANode(int nodeId) {
        List<String> files = new ArrayList<>();
        for(Map.Entry<String,Integer> entry: replicaMap.entrySet()){
                if(entry.getValue() == nodeId){
                    files.add(entry.getKey());
                    replicaMap.remove(entry.getKey());
                }
        }
        return files;
    }

    public static void addOwnedFiles(List<String>ownedFiles) {
        FileData.ownedFiles.addAll(ownedFiles);
    }
}
