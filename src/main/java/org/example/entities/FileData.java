package org.example.entities;

import java.util.List;
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

    public static boolean checkReplica(String fileName) {
        return replicaMap.containsKey(fileName) || ownedFiles.contains(fileName);
    }
}
