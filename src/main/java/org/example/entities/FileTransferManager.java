package org.example.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class FileTransferManager {
    private static final RequestQueue requestQueue = new RequestQueue();
    private static final List<String> eventLog = new ArrayList<>();
    private static ConcurrentSkipListMap<String, List<String >> fileOperations = new ConcurrentSkipListMap<>();

    public static RequestQueue getRequestQueue() {
        return requestQueue;
    }

    public static synchronized void logEvent(String event) {
        eventLog.add(event);
        String[] parts = event.split(" : ");

        if (parts.length < 4) {
            System.out.println("Invalid format");
            return;
        }

        String operationType = parts[1];
        String status = parts[2];
        String fileName = parts[3];
        if(status.equals("Successful")) {
            if (fileOperations.containsKey(fileName)) {
                fileOperations.get(fileName).add(operationType);
            }else{
                fileOperations.put(fileName, new ArrayList<>());
                fileOperations.get(fileName).add(operationType);
            }
        }

        System.out.println("Event: " + event);
    }

    public static List<String> getEventLog() {
        return eventLog;
    }

    public static List<String> getFileOperations(String fileName) {
        return fileOperations.get(fileName);
    }

    public static ConcurrentSkipListMap<String, List<String>> getFileOperations() {
        return fileOperations;
    }

    //TODO compare two file operations
}
