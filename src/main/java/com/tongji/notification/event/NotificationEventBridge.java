package com.tongji.notification.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 计数事件 → 通知事件桥接器。
 *
 * <p>监听 {@link com.tongji.counter.event.CounterServiceImpl#toggle} 发出的本地事件，
 * 仅在正向动作（delta &gt; 0，即点赞/收藏而非取消）且实体为知文时，
 * 转发为通知事件到 Kafka。取消点赞、取消收藏不产生通知。</p>
 *
 * <p>本监听器在点赞请求线程内同步执行，但只做一次非阻塞 kafka.send，
 * 且任何异常都被吞掉——通知属于旁路能力，绝不影响点赞主链路。</p>
 */
@Component
public class NotificationEventBridge {
    private static final Logger log = LoggerFactory.getLogger(NotificationEventBridge.class);

    private final NotificationProducer producer;

    public NotificationEventBridge(NotificationProducer producer) {
        this.producer = producer;
    }

    /**
     * 监听本地计数事件并转换为通知事件。
     * @param evt 计数事件（含 userId/metric/entityType/entityId/delta）
     */
    @EventListener
    public void onCounterEvent(com.tongji.counter.event.CounterEvent evt) {
        try {
            if (evt.getDelta() <= 0) {
                return; // unlike/unfav 不通知
            }
            if (!"knowpost".equals(evt.getEntityType())) {
                return; // 当前接收者解析仅支持知文实体
            }
            producer.publish(NotificationEvent.of(
                    java.util.UUID.randomUUID().toString(),
                    evt.getUserId(),
                    evt.getMetric(),   // like / fav，与 actionType 天然对应
                    evt.getEntityType(),
                    evt.getEntityId()));
        } catch (Exception ex) {
            log.warn("Notify event publish failed, post {}: {}", evt.getEntityId(), ex.getMessage());
        }
    }
}
//TODO
//ES搜索问题