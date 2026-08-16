package com.tongji.knowpost.service;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
@RequiredArgsConstructor
public class FeedCacheService {

    private final StringRedisTemplate redis;

    /**
     * 删除公共 Feed 缓存键空间。
     *
     * <p>键模式: `feed:public:*`</p>
     * <p>用途: 页面结构调整、聚合逻辑升级或批量失效时，清空公共 Feed 缓存。</p>
     * <p>注意: 当前采用 `redis.keys` 遍历，适用于小规模键空间; 生产建议维护索引集合或使用扫描以降低开销
     */
    public void deleteAllFeedCaches() {
        Set<String> keys = redis.keys("feed:public:*");
        if (!keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    /**
     * 公共 Feed 缓存的双删 (Double-Delete) 策略。
     *
     * <p>流程: 先删除 → 等待短延迟 → 再删除一次。</p>
     * <p>目的: 降低并发回填或异步写入导致的脏数据残留风险。</p>
     *
     * @param delayMillis 两次删除之间的延迟（最小 50ms）
     */
    public void doubleDeleteAll(long delayMillis) {
        deleteAllFeedCaches();
        try {
            Thread.sleep(Math.max(delayMillis, 50));
        } catch (InterruptedException ignored) {
        }
        deleteAllFeedCaches();
    }

    /**
     * 删除某用户的个人 Feed 缓存键空间。
     *
     * <p>键模式: `feed:mine:{userId}:*`</p>
     * <p>用途: 用户个性化配置、关注关系等发生变化时，清空该用户的个性化 Feed 缓存。</p>
     */
    public void deleteMyFeedCaches(long userId) {
        Set<String> keys = redis.keys("feed:mine:" + userId + ":*");
        if (!keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    /**
     * 某用户个人 Feed 缓存的双删 (Double-Delete) 策略。
     *
     * <p>流程: 先删除 → 等待短延迟 → 再删除一次。</p>
     * <p>目的: 在个性化缓存可能被并发回填的场景，提升失效的确定性。</p>
     *
     * @param userId      用户 ID
     * @param delayMillis 两次删除之间的延迟（最小 50ms）
     */
    public void doubleDeleteMy(long userId, long delayMillis) {
        deleteMyFeedCaches(userId);
        try {
            Thread.sleep(Math.max(delayMillis, 50));
        } catch (InterruptedException ignored) {
        }
        deleteMyFeedCaches(userId);
    }

}