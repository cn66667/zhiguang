package com.tongji.notification.event;

import lombok.Data;

/**
 * 通知事件模型。
 *
 * <p>描述一次正向行为（点赞/收藏），由事件桥接器发到 Kafka，
 * 消费者解析接收者后落库并通过 SSE 实时推送。</p>
 *
 * <p>注意：事件只携带触发者（actor），不携带接收者——
 * 接收者在消费端按实体异步解析，保持生产行为路径低延迟。</p>
 */
@Data
public class NotificationEvent {
    private String eventId;     // 全局唯一事件 ID（消费幂等去重键）
    private long actorId;       // 触发者用户 ID
    private String actionType;  // like | fav
    private String entityType;  // knowpost
    private String entityId;
    private long ts;            // 产生时间戳（毫秒）

    public NotificationEvent(String eventId, long actorId, String actionType, String entityType, String entityId, long ts) {
        this.eventId = eventId;
        this.actorId = actorId;
        this.actionType = actionType;
        this.entityType = entityType;
        this.entityId = entityId;
        this.ts = ts;
    }

    public static NotificationEvent of(String eventId, long actorId, String actionType, String entityType, String entityId) {
        return new NotificationEvent(eventId, actorId, actionType, entityType, entityId, System.currentTimeMillis());
    }
}
