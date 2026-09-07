package com.tongji.notification.config;

import com.tongji.notification.event.NotificationTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 通知模块配置。
 *
 * <p>声明 notification-events 主题，由 KafkaAdmin 自动创建，
 * 避免依赖 broker 的自动建题开关。</p>
 */
@Configuration
public class NotificationConfig {

    @Bean
    public NewTopic notificationEventsTopic() {
        return TopicBuilder.name(NotificationTopics.EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
