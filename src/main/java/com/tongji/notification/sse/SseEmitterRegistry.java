package com.tongji.notification.sse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SSE 连接注册表（有状态，单机实现）。
 *
 * <p>职责：维护 userId → SseEmitter 的内存映射，提供注册与推送。</p>
 *
 * <p>设计要点：</p>
 * - 连接是内存态资源，必须在 onCompletion/onTimeout/onError 回调中移除，防止泄漏；
 * - 同一用户重连时顶替旧连接，避免一个用户挂多条连接；
 * - send 失败（客户端已断但回调未触发）就地清理并 complete，防止后续推送持续抛错；
 * - 心跳每 30s 发注释帧保活：对抗网关/LB 的 idle timeout，注释帧不会进入前端 onmessage。
 *
 * <p>多实例扩展位：连接落在哪台实例上不可知时，
 * 将本类改为「本地查找 + Redis Pub/Sub 广播」，业务调用方无需改动。</p>
 */
@Component
public class SseEmitterRegistry {
    private static final Logger log = LoggerFactory.getLogger(SseEmitterRegistry.class);

    /** 连接超时 30min，前端 EventSource 会自动重连重建 */
    private static final long TIMEOUT_MILLIS = 30 * 60 * 1000L;

    private final Map<Long, SseEmitter> emitters = new ConcurrentHashMap<>();

    /**
     * 注册当前用户的推送通道。
     * @param userId 用户 ID
     * @return 新建的 SseEmitter
     */
    public SseEmitter register(long userId) {
        // 同一用户新连接顶替旧连接，防止重复注册导致泄漏
        SseEmitter old = emitters.remove(userId);
        if (old != null) {
            try {
                old.complete();
            } catch (Exception ignored) {}
        }
        SseEmitter emitter = new SseEmitter(TIMEOUT_MILLIS);
        //客户端主动断开连接
        emitter.onCompletion(() -> emitters.remove(userId, emitter));
        //超时断开
        emitter.onTimeout(() -> emitters.remove(userId, emitter));
        emitter.onError(e -> emitters.remove(userId, emitter));
        //为了防止内存泄露
        emitters.put(userId, emitter);
        return emitter;
    }

    /**
     * 向在线用户推送一条事件；离线则静默忽略（落库兜底，列表可查）。
     * @param userId 用户 ID
     * @param eventName 事件名（前端 addEventListener 对应）
     * @param jsonData 已序列化的 JSON 字符串
     */
    public void send(long userId, String eventName, String jsonData) {
        SseEmitter emitter = emitters.get(userId);
        if (emitter == null) {
            return; // 不在线：不推。DB 里已有记录，打开通知列表仍可见
        }
        try {
            emitter.send(SseEmitter.event().name(eventName).data(jsonData));
        } catch (Exception ex) {
            log.debug("SSE send failed for user {}, cleanup: {}", userId, ex.getMessage());
            emitters.remove(userId, emitter);
            try {
                emitter.completeWithError(ex);
            } catch (Exception ignored) {}
        }
    }

    /**
     * 当前在线连接数（观测用）。
     */
    public int onlineCount() {
        return emitters.size();
    }

    /**
     * 心跳保活：固定 30s 给所有连接发注释帧，喂给中间的网关/LB 看「连接还活着」。
     */
    @Scheduled(fixedDelay = 30000L, initialDelay = 30000L)
    public void heartbeat() {
        if (emitters.isEmpty()) {
            return;
        }
        for (Map.Entry<Long, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(SseEmitter.event().comment("ping"));
            } catch (Exception ex) {
                Long userId = entry.getKey();
                SseEmitter emitter = entry.getValue();
                emitters.remove(userId, emitter);
                try {
                    emitter.completeWithError(ex);
                } catch (Exception ignored) {}
            }
        }
    }
}
