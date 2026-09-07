package com.tongji.notification.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPost;
import com.tongji.notification.mapper.NotificationMapper;
import com.tongji.notification.model.NotificationRow;
import com.tongji.notification.schema.NotificationKeys;
import com.tongji.notification.sse.SseEmitterRegistry;
import com.tongji.user.domain.User;
import com.tongji.user.mapper.UserMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 通知消费者。
 *
 * <p>职责：消费正向行为事件，解析接收者（知文作者）后落库、累加未读红点，
 * 并通过 SSE 实时推送给在线作者；全部成功后才手动提交位点。</p>
 *
 * <p>顺序设计：先落库、再推送、最后 ack——保证「通知不丢」：
 * 推送时人不在线也没关系，库里已有记录，打开通知列表仍可见。</p>
 */
@Service
public class NotificationConsumer {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redis;
    private final KnowPostMapper knowPostMapper;
    private final UserMapper userMapper;
    private final NotificationMapper notificationMapper;
    private final SseEmitterRegistry sseRegistry;

    public NotificationConsumer(ObjectMapper objectMapper,
                                StringRedisTemplate redis,
                                KnowPostMapper knowPostMapper,
                                UserMapper userMapper,
                                NotificationMapper notificationMapper,
                                SseEmitterRegistry sseRegistry) {
        this.objectMapper = objectMapper;
        this.redis = redis;
        this.knowPostMapper = knowPostMapper;
        this.userMapper = userMapper;
        this.notificationMapper = notificationMapper;
        this.sseRegistry = sseRegistry;
    }

    /**
     * 消费通知事件（手动 ack 模式）。
     * <p>异常分类：</p>
     * - event_id 冲突 → 幂等场景，正常确认；
     * - JSON 解析失败 / ID 非数字 → 毒消息，记录并跳过位点，避免无限重试卡死分区；
     * - 其余（DB/Redis 抖动）→ 不提交位点，等待 Kafka 重投。
     *
     * @param message 事件 JSON
     * @param ack 位点确认对象
     */
    @KafkaListener(topics = NotificationTopics.EVENTS, groupId = "notification-consumer")
    public void onMessage(String message, Acknowledgment ack) throws Exception {
        try {
            NotificationEvent evt = objectMapper.readValue(message, NotificationEvent.class);
            process(evt);
            ack.acknowledge();
        } catch (DuplicateKeyException dup) {
            // 同一 eventId 已处理过：Kafka 至少一次投递的重复消息，幂等跳过
            ack.acknowledge();
        } catch (JsonProcessingException | NumberFormatException bad) {
            // 永久不可处理的消息：跳过位点防阻塞，接入告警排查生产端
            log.warn("Drop poison notification message: {}", message);
            ack.acknowledge();
        } catch (Exception ex) {
            // 瞬时故障：不提交位点，等待重投，不ACK
            log.warn("Process notification failed, will retry: {}", ex.getMessage());
        }
    }

    /**
     * 处理单条事件：解析接收者 → 落库 → 红点 +1 → SSE 实时推送。
     */
    private void process(NotificationEvent evt) throws Exception {
        long postId = Long.parseLong(evt.getEntityId());
        KnowPost post = knowPostMapper.findById(postId);
        if (post == null || post.getCreatorId() == null) {
            return; // 内容已删除等场景无接收者，直接丢弃
        }
        long recipient = post.getCreatorId();
        if (recipient == evt.getActorId()) {
            return; // 自己给自己点赞/收藏不通知
        }

        NotificationRow row = new NotificationRow();
        row.setEventId(evt.getEventId());
        row.setRecipientId(recipient);
        row.setActorId(evt.getActorId());
        row.setActionType(evt.getActionType());
        row.setEntityType(evt.getEntityType());
        row.setEntityId(postId);
        row.setContent(buildContent(evt.getActionType(), post.getTitle()));
        notificationMapper.insert(row); // 唯一键兜底幂等：重复事件在 insert 处被拦截

        try {
            // 未读红点计数（缓存）：键不存在时读接口会回源 notification 表自愈
            redis.opsForValue().increment(NotificationKeys.unreadKey(recipient));
        } catch (Exception ex) {
            log.warn("Unread incr failed for {}: {}", recipient, ex.getMessage());
        }

        pushRealtime(recipient, evt, post.getTitle());
    }

    /**
     * SSE 实时推送（在线才推得出去，尽力而为）。
     */
    private void pushRealtime(long recipient, NotificationEvent evt, String title) {
        try {
            String actorNickname = resolveActorNickname(evt.getActorId());
            //线程不安全？
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("eventId", evt.getEventId());
            payload.put("actorId", evt.getActorId());
            payload.put("actorNickname", actorNickname);
            payload.put("actionType", evt.getActionType());
            payload.put("entityType", evt.getEntityType());
            payload.put("entityId", evt.getEntityId());
            payload.put("title", title);
            payload.put("content", buildContent(evt.getActionType(), title));
            payload.put("ts", evt.getTs());
            sseRegistry.send(recipient, "notification", objectMapper.writeValueAsString(payload));
        } catch (Exception ex) {
            log.debug("SSE realtime push failed for {}: {}", recipient, ex.getMessage());
        }
    }

    private String resolveActorNickname(long actorId) {
        try {
            User actor = userMapper.findById(actorId);
            return actor != null ? actor.getNickname() : null;
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * 组装文案快照。
     */
    private String buildContent(String actionType, String title) {
        String action = switch (actionType) {
            case "like" -> "赞了";
            case "fav" -> "收藏了";
            default -> actionType + "了";
        };
        return (title == null || title.isBlank())
                ? action + "你的知文"
                : action + "你的知文《" + title + "》";
    }
}
