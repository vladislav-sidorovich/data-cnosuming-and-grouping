package com.example.kafkaload.container;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.example.kafkaload.Notification;

@Component
public class NotificationsContainer {
    private Map<String, Notification> notifications = new ConcurrentHashMap<>();

    public void add(Notification notification) {
        if (notification == null) {
            return;
        }
        if (notification.getGame() == null) {
            return;
        }
        notifications.put(notification.getGame(), notification);
    }

    public int size() {
        return notifications.size();
    }
}
