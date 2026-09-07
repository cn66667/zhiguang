package com.tongji.notification.api;

import com.tongji.auth.token.JwtService;
import com.tongji.notification.mapper.NotificationMapper;
import com.tongji.notification.schema.NotificationKeys;
import com.tongji.notification.sse.SseEmitterRegistry;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 通知接口：SSE 实时流、列表、未读数、全部已读。
 *
 * <p>鉴权说明：EventSource 无法携带 Authorization 头，
 * /stream 的 access token 走查询参数 ?token=xxx 传入，由本控制器手动校验
 * （SecurityConfig 对该路径 permitAll）；token 为一次性校验值，绝不入日志。
 * 其余接口仍走全局 JWT 过滤链。</p>
 */
@RestController
@RequestMapping("/api/v1/notification")
public class NotificationController {

    private static final int MAX_LIMIT = 50;

    private final SseEmitterRegistry registry;
    private final JwtService jwtService;
    private final NotificationMapper notificationMapper;
    private final StringRedisTemplate redis;

    public NotificationController(SseEmitterRegistry registry,
                                  JwtService jwtService,
                                  NotificationMapper notificationMapper,
                                  StringRedisTemplate redis) {
        this.registry = registry;
        this.jwtService = jwtService;
        this.notificationMapper = notificationMapper;
        this.redis = redis;
    }

    /**
     * SSE 订阅端点。
     *
     * <p>连接后先发一帧 connected 事件确认握手；此后服务端在产生通知时推
     * notification 事件（JSON 字符串），另有 30s 一次的注释帧心跳。</p>
     *
     * @param token 查询参数中的 access token
     * @param response 原始响应（用于校验失败时直接写 401）
     * @return SseEmitter（异步写入），失败返回 null（响应已被接管）
     */
    @GetMapping("/stream")
    public Object stream(@RequestParam(value = "token", required = false) String token,
                         HttpServletResponse response) throws IOException {
        //从token解析userId
        Long userId = resolveUserId(token);
        if (userId == null) {
            writeUnauthorized(response);
            return null;
        }
        //emitter:操作长连接的控制器
        SseEmitter emitter = registry.register(userId);
        // 握手帧：前端收到后即可确认订阅成功并拉取一次未读数/列表
        registry.send(userId, "connected", "{\"ok\":true}");
        return emitter;
    }

    /**
     * 通知列表（触发者昵称/头像联查），按时间倒序分页。
     */
    //@AuthenticationPrincipal-自动从请求头获取token,解析为jwt
    @GetMapping("/list")
    public ResponseEntity<Map<String, Object>> list(@AuthenticationPrincipal Jwt jwt,
                                                    @RequestParam(defaultValue = "20") int limit,
                                                    @RequestParam(defaultValue = "0") int offset) {
        long uid = jwtService.extractUserId(jwt);
        int safeLimit = Math.min(Math.max(limit, 1), MAX_LIMIT);
        int safeOffset = Math.max(offset, 0);
        List<Map<String, Object>> items = notificationMapper.listByRecipient(uid, safeLimit, safeOffset);
        long unread = getUnread(uid);
        return ResponseEntity.ok(Map.of(
                "items", items,
                "unreadCount", unread
        ));
    }

    /**
     * 未读红点数。优先读 Redis 计数；键缺失时回源 DB 并回填自愈。
     */
    @GetMapping("/unread")
    public ResponseEntity<Map<String, Object>> unread(@AuthenticationPrincipal Jwt jwt) {
        long uid = jwtService.extractUserId(jwt);
        return ResponseEntity.ok(Map.of("count", getUnread(uid)));
    }

    /**
     * 全部标记已读：DB 置位 is_read，同时清掉 Redis 红点计数重新累计。
     */
    @PostMapping("/read")
    public ResponseEntity<Map<String, Object>> readAll(@AuthenticationPrincipal Jwt jwt) {
        long uid = jwtService.extractUserId(jwt);
        notificationMapper.markAllRead(uid);
        try {
            redis.delete(NotificationKeys.unreadKey(uid));
        } catch (Exception ignored) {}
        return ResponseEntity.ok(Map.of("success", true));
    }
    /**
     * 单条标记已读：DB 置位 is_read，同时清掉 Redis 红点计数重新累计。
     */
    @PostMapping("/readOne")
    public ResponseEntity<Map<String, Object>> readOne(@AuthenticationPrincipal Jwt jwt,Long entityId,String entityType) {
        long uid = jwtService.extractUserId(jwt);
        int n=notificationMapper.markOneRead(uid,entityId,entityType);
        if(n>0)
        {
            decrementUnread(uid,n);
        }
        return ResponseEntity.ok(Map.of("success", true,
                                        "unreadCount",getUnread(uid)));
    }
    /**
     * 手动校验查询参数里的 token，仅接受 access 类型。
     * @return 用户 ID；非法则 null
     */
    private Long resolveUserId(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        try {
            Jwt jwt = jwtService.decode(token); // 过期/签名错误由 decoder 抛异常
            if (!"access".equals(jwtService.extractTokenType(jwt))) {
                return null; // 不允许用 refresh token 建立推送长连
            }
            return jwtService.extractUserId(jwt);
        } catch (JwtException | IllegalArgumentException ex) {
            return null;
        }
    }

    /**
     * 未读数：Redis 缓存优先，缺失时回源统计。
     * <p>注：回填与消费端 INCR 存在极小并发窗口，偏差会在下一次打开列表时收敛。</p>
     */
    private long getUnread(long uid) {
        String key = NotificationKeys.unreadKey(uid);
        try {
            String cached = redis.opsForValue().get(key);
            if (cached != null) {
                return Long.parseLong(cached);
            }
        } catch (Exception ex) {
            // Redis 故障降级走 DB
        }
        long count = notificationMapper.countByRecipient(uid);
        try {
            redis.opsForValue().set(key, String.valueOf(count), Duration.ofDays(7));
        } catch (Exception ignored) {}
        return count;
    }
    private void decrementUnread(long uid,int delta) {
        String key = NotificationKeys.unreadKey(uid);
        try{
            Long v=redis.opsForValue().increment(key,-delta);
            if(v!=null&&v<0)
                redis.opsForValue().set(key,"0");
        }
        catch(Exception ignored){}
    }

    private void writeUnauthorized(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json;charset=UTF-8");
        response.getWriter().write("{\"code\":\"UNAUTHORIZED\",\"message\":\"未登录或令牌无效\"}");
    }
}
