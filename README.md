# 知光平台（ZhiGuang）— 知识获取与分享社区

一个面向高校/知识社区的 **知识分享 APP 后端**：支持发布知识笔记（知文）、点赞/收藏、关注取关、首页 Feed 流、对象存储直传，以及基于 RAG 的 AI 摘要与知识问答。各模块面向高并发、高可用进行了充分设计。

- **后端仓库**：https://github.com/cn66667/zhiguang

## 技术栈

| 层级 | 技术 |
|------|------|
| 语言 / 框架 | Java 21 · Spring Boot 3.2 · Spring Security · Spring AI 1.0 |
| 数据库 / 缓存 | MySQL 8 · Redis 7 · Caffeine |
| 消息 / 事件 | Kafka · Canal（MySQL binlog → Outbox） |
| 存储 / 搜索 | 阿里云 OSS · Elasticsearch 8（含向量检索） |
| 对象映射 | MyBatis · Redisson（分布式锁 / 限流） |
| AI | DeepSeek（文本生成）· 通义 text-embedding-v4（向量）· RAG |

## 核心亮点

- **认证系统**：Spring Security + JWT 双令牌，RS256 签名 + Redis 刷新令牌白名单，15 分钟访问令牌 + 7 天刷新令牌，支持即时撤销。
- **计数系统**：笔记维度（点赞/收藏）与用户维度（关注/粉丝）以 Redis 为底层存储，采用定制化 SDS 二进制紧凑计数 + Lua 原子更新，内置采样一致性校验与自愈重建。
- **发布系统**：渐进式发布流程，图片/视频/Markdown 正文直传 OSS，采用后端预签名 + 前端直传，接入 DeepSeek 一键生成文章摘要。
- **用户关系系统**：一主多从 + 事件驱动。关注事件与 Outbox 表同事务写入，Canal 订阅 binlog 发布到 Kafka，异步更新粉丝表、计数、列表缓存。
- **点赞系统**：异步写 + Kafka 写聚合应对高并发写；分片位图高效幂等与判重，读取缺失时按需重建，Kafka 做灾难回放兜底。
- **Feed 流**：Caffeine + Redis 页面缓存 + Redis 片段缓存三级架构；自定义 hotkey 探测，按热点层级延长缓存时长并叠加随机抖动抗雪崩；single-flight 单飞锁避免并发回源风暴。
- **搜索系统**：Elasticsearch 内容搜索与联想建议，`search_after` 深分页，`function_score` 融合 BM25 与点赞等业务权重，`completion suggester` 前缀联想。
- **AI 问答（RAG）**：接口调用 → 索引检查 → 向量检索 → Prompt 构造 → 大模型流式生成全流程，通过合理分块、幂等删除保持单一版本、预索引减少首次提问等待。

## 项目结构

```
zhiguang_be
├── src/main/java/com/tongji
│   ├── ZhiGuangApplication.java    # 启动类
│   ├── auth/                       # 认证：注册/登录/JWT/验证码/登录日志
│   ├── user/                       # 用户领域与持久化
│   ├── profile/                    # 用户资料 / 头像上传 / CORS
│   ├── knowpost/                   # 知文：发布/编辑/Feed/雪花ID/缓存失效
│   ├── counter/                    # 计数：SDS 计数 / 位图 / 聚合消费 / 重建
│   ├── relation/                   # 用户关系：关注/粉丝/Outbox/Canal→Kafka
│   ├── search/                     # 搜索：ES 索引/检索/联想/搜索Outbox
│   ├── llm/                        # AI：摘要生成 / RAG 向量索引与检索
│   ├── storage/                    # 对象存储：OSS 预签名直传
│   ├── cache/                      # 缓存：Caffeine 配置 / 热点探测
│   ├── common/                     # 通用：异常 / Web 工具 / Outbox 工具
│   └── config/                     # 全局配置：ES/Redisson/线程池
├── src/main/resources
│   ├── application.yml             # 主配置（需填密钥）
│   ├── mapper/                     # MyBatis XML
│   └── keys/                       # JWT 公私钥（RS256）
├── db/schema.sql                   # MySQL 建表脚本
├── docker/                         # 中间件容器配置（MySQL 授权 / Canal 配置）
├── docs/                           # 设计文档与 API 文档
└── docker-compose.yml              # 中间件一键编排
```
