package com.tongji.notification.model;

import lombok.Data;

/**
 * 通知落库行模型（写入参数对象）。
 *
 * <p>事件由消费端补全接收者与文案快照后写库；列表查询返回 Map 行，不走此模型。</p>
 */
@Data
public class NotificationRow {
    private String eventId;     // 幂等去重键（uk_notification_event）
    private Long recipientId;   // 接收者（作者）
    private Long actorId;       // 触发者
    private String actionType;  // like | fav
    private String entityType;  // knowpost
    private Long entityId;
    private String content;     // 文案快照
}
