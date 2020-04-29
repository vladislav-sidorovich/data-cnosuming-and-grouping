package com.example.kafkaload.container;

import com.example.kafkaload.Notification;

public interface NotificationsContainer {
    void add(Notification notification);

    int size();
}
