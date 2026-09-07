package com.tongji.notification.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * 通知事件生产者。
 *
 * <p>职责：将正向行为事件异步发送到 Kafka 主题，供通知消费者处理（落库 + SSE 推送）。</p>
 */
@Service
public class NotificationProducer {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;

    public NotificationProducer(KafkaTemplate<String, String> kafka, ObjectMapper objectMapper) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
    }

    /**
     * 发布通知事件到 Kafka（kafka.send 异步，不阻塞调用方）。
     * @param event 通知事件
     */
    public void publish(NotificationEvent event) {
        try {
            // kafka 只认字符串/字节数组，先序列化
            String payload = objectMapper.writeValueAsString(event);
            kafka.send(NotificationTopics.EVENTS, payload);
        } catch (JsonProcessingException e) {
            // 序列化失败不影响主流程；可接入告警
        }
    }
}
