package org.example.entities;

import java.util.ArrayList;
import java.util.List;

public class FileTransferManager {
    private static final RequestQueue requestQueue = new RequestQueue();
    private static final List<String> eventLog = new ArrayList<>();

    public static RequestQueue getRequestQueue() {
        return requestQueue;
    }

    public static synchronized void logEvent(String event) {
        eventLog.add(event);
        System.out.println("Event: " + event);
    }

    public static List<String> getEventLog() {
        return eventLog;
    }
}
