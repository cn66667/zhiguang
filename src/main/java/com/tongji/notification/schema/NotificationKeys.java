package com.tongji.notification.schema;

/**
 * 通知模块 Redis 键规范。
 *
 * <p>未读红点计数键：notif:unread:{userId}。
 * 事实源为 notification 表（is_read），红点计数是秒级最终一致的缓存。</p>
 */
public final class NotificationKeys {

    /** 未读红点计数键前缀 */
    public static final String UNREAD_PREFIX = "notif:unread:";

    public static String unreadKey(long userId) {
        return UNREAD_PREFIX + userId;
    }

    private NotificationKeys() {}
}
