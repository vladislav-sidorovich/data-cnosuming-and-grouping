package com.example.kafkaload.container;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.example.kafkaload.Notification;

@Component
public class NotificationsContainerMapImpl implements NotificationsContainer {
    private static final Logger logger = LoggerFactory.getLogger(NotificationsContainerMapImpl.class);
    private static final int capacity = 1_000_000;

    private Map<String, Notification> notifications = new ConcurrentHashMap<>();

    @Override
    public void add(Notification notification) {
        if (notification == null) {
            return;
        }
        if (notification.getGame() == null) {
            return;
        }

//        makeTrash(notification);

        notifications.put(notification.getGame(), notification);
    }

    private void makeTrash(Notification notification) {
        String data = notification.getData();
        String[] arr = data.split("\\s");

        List<String> list = Arrays.asList(arr);
    }

    @Override
    public int size() {
        return notifications.size();
    }

    private List<String> newQueue() {
        return new ArrayList<>(capacity);
    }
}
