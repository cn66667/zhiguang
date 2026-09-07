package com.tongji.notification.mapper;

import com.tongji.notification.model.NotificationRow;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * 通知表数据访问。
 *
 * <p>插入依赖 event_id 唯一键做消费幂等：重复投递时触发 DuplicateKeyException，
 * 由消费者识别并跳过。</p>
 */
@Mapper
public interface NotificationMapper {

    int insert(NotificationRow row);

    // 通知列表（带触发者昵称/头像），按时间倒序分页
    List<Map<String, Object>> listByRecipient(@Param("recipientId") long recipientId,
                                              @Param("limit") int limit,
                                              @Param("offset") int offset);

    // 未读条数（Redis 键缺失时的回源统计）
    long countByRecipient(@Param("recipientId") long recipientId);

    // 全部标记已读
    int markAllRead(@Param("recipientId") long recipientId);
    //单条标记已读
    int markOneRead(@Param("recipientId") long recipientId
                  , @Param("entityId") long entityId,
                    @Param("entityType")String entityType);
}
