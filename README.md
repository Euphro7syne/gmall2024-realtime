# Gmall2024 实时数仓项目

## 项目简介

本项目是基于 Apache Flink 构建的电商实时数据仓库系统，采用经典的数仓分层架构（ODS → DWD → DWS → ADS），实现了对电商业务数据的实时采集、处理和分析。

## 技术栈

- **计算引擎**: Apache Flink 1.17.1
- **编程语言**: Java 1.8
- **消息队列**: Apache Kafka
- **数据存储**:
  - Apache HBase (维度存储)
  - Apache Doris (OLAP 分析)
  - Redis (缓存)
- **数据同步**: Flink CDC 2.4.2
- **构建工具**: Maven
- **Web 应用**: Spring Boot (gmall2024-publisher)

## 项目结构

```
gmall2024-realtime/
├── realtime-common/          # 公共模块
│   └── 包含常量定义、通用 Bean、自定义函数等
├── realtime-dim/             # 维度层
│   └── 维度数据加工处理
├── realtime-dwd/             # 明细层
│   ├── realtime-dwd-base-db/       # 业务数据库明细
│   ├── realtime-dwd-base-log/      # 日志数据明细
│   ├── realtime-dwd-trade-order-detail/        # 交易订单明细
│   ├── realtime-dwd-trade-order-cancel-detail/ # 取消订单明细
│   ├── realtime-dwd-trade-order-pay-suc-detail/# 支付成功订单明细
│   ├── realtime-dwd-trade-order-refund/        # 退款订单明细
│   └── ...其他业务域明细
├── realtime-dws/             # 服务层
│   ├── realtime-dws-user-user-login-window/    # 用户登录聚合
│   ├── realtime-dws-trade-cart-add-uu-window/  # 加购 UV 聚合
│   ├── realtime-dws-traffic-*                  # 流量相关聚合
│   └── ...其他主题聚合
├── gmall2024-publisher/      # 数据发布服务
│   └── 提供 RESTful API 接口，支持前端数据可视化
└── pom.xml                   # 父工程配置
```

## 模块说明

### 1. realtime-common (公共模块)

提供项目通用的组件和工具类：

- 常量定义 (`Constant.java`)
- 通用 Bean 类 (各层级数据模型)
- 自定义维表关联函数 (`DimJoinFunction`)
- 表处理流程配置类

### 2. realtime-dim (维度层)

负责维度数据的处理和存储：

- 从 Kafka 读取维度变更数据
- 通过 Flink CDC 同步 MySQL 维度表
- 将维度数据写入 HBase 供 DWD 层关联查询

**主应用**: `DimApp.java`

### 3. realtime-dwd (明细层)

构建业务过程的事实明细表：

#### 基础数据

- **DwdBaseDb**: 业务数据库多流合并，按表分流处理
- **DwdBaseLog**: 日志数据分流处理（启动页、曝光页、错误日志等）

#### 交易域

- **DwdTradeOrderDetail**: 下单事务事实表
- **DwdTradeOrderCancelDetail**: 取消订单事实表
- **DwdTradeOrderPaySucDetail**: 支付成功事实表
- **DwdTradeOrderRefund**: 退款事实表
- **DwdTradeRefundPaySucDetail**: 退款支付成功事实表

#### 互动域

- **DwdInteractionCommentInfo**: 评论事实表

### 4. realtime-dws (服务层)

基于 DWD 层数据进行轻度聚合，构建主题宽表：

#### 用户主题

- **DwsUserUserLoginWindow**: 用户登录聚合（分钟级窗口）
- **DwsUserUserRegisterWindow**: 用户注册聚合

#### 交易主题

- **DwsTradeCartAddUuWindow**: 加购 UV 聚合
- **DwsTradeSkuOrderWindow**: SKU 粒度下单聚合
- **DwsTradeProvinceOrderWindow**: 省份粒度下单聚合

#### 流量主题

- **DwsTrafficVcChArIsNewPageViewWindow**: 多维度页面浏览聚合
- **DwsTrafficHomeDetailPageViewWindow**: 首页/详情页浏览聚合
- **DwsTrafficSourceKeywordPageViewWindow**: 来源关键词页面浏览聚合
  - 使用 IK 分词器进行关键词提取
  - 自定义 UDTF 实现关键词拆分

### 5. gmall2024-publisher (数据发布服务)

基于 Spring Boot 的数据服务应用：

- 集成 MyBatis 访问 Doris/ClickHouse 等 OLAP 引擎
- 提供 RESTful API 接口
- 支持实时数据大屏展示

**主要功能**:

- 流量统计接口 (`TrafficStatsController`)
- 交易统计接口 (`TradeStatsController`)
- 各维度数据分析接口

## 核心特性

1. **实时性**: 基于 Flink 流式计算，实现秒级延迟的数据处理
2. **准确性**: 使用精确一次（Exactly-Once）语义，确保数据准确
3. **可扩展性**: 模块化设计，支持水平扩展
4. **容错性**: 基于 Checkpoint 机制实现故障恢复
5. **维度关联**: 支持异步维表查询，优化关联性能

## 数据流转

```
MySQL/Binlog ──► Kafka ──► Flink CDC ──► DWD 层 ──► Kafka
                                                      │
                                                      ▼
                                                   DWS 层 ──► Kafka
                                                              │
                                                              ▼
                                                           Doris/ClickHouse
                                                              │
                                                              ▼
                                                         Publisher API
                                                              │
                                                              ▼
                                                          前端可视化
```

## 环境要求

- JDK 1.8+
- Maven 3.6+
- Flink 1.17.1
- Kafka 2.x+
- HBase 2.4+
- Doris 1.5+
- MySQL 5.7+

## 构建与运行

### 编译打包

```bash
mvn clean package -DskipTests
```

### 提交 Flink 任务

```bash
flink run -c com.atguigu.gmall.realtime.dim.app.DimApp realtime-dim.jar
flink run -c com.atguigu.gmall.realtime.dwd.db.split.app.DwdBaseDb realtime-dwd-base-db.jar
# ... 其他模块类似
```

### 启动 Publisher 服务

```bash
cd gmall2024-publisher
java -jar target/gmall2024-publisher.jar
```

## 配置说明

主要配置项在 `pom.xml` 和各模块的配置文件中：

| 配置项          | 版本   | 说明         |
| --------------- | ------ | ------------ |
| Flink           | 1.17.1 | 流计算引擎   |
| Flink CDC       | 2.4.2  | 数据同步组件 |
| Hadoop          | 3.3.4  | 分布式存储   |
| HBase           | 2.4.11 | NoSQL 数据库 |
| FastJSON        | 1.2.83 | JSON 处理    |
| Doris Connector | 1.5.2  | Doris 连接器 |

## 开发规范

1. 所有 Flink 任务需配置 Checkpoint
2. 使用侧输出流处理脏数据
3. 维度关联优先使用异步 I/O
4. 状态管理需设置 TTL
5. 生产环境需开启 Savepoint

## 常见问题

### Q: 如何处理数据倾斜？

A: 使用 LocalKeyBy、Rebalance 或自定义分区策略

### Q: 维表关联性能如何优化？

A: 使用异步维表查询 + Redis/HBase 缓存

### Q: 如何保证端到端一致性？

A: 开启 TwoPhaseCommitSink + Checkpoint

## 许可证

本项目仅供学习交流使用

## 联系方式

如有问题，请联系项目维护者。
