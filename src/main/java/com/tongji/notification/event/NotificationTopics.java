package com.tongji.notification.event;

/**
 * 通知模块 Kafka 主题常量。
 */
public final class NotificationTopics {
    /** 通知事件主题（点赞/收藏等正向行为） */
    public static final String EVENTS = "notification-events";
    private NotificationTopics() {}
}
