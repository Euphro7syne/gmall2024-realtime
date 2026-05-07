
# Gmall2024 实时数仓 Kubernetes 部署方案

## 一、部署架构

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                        │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Kafka     │  │   HBase     │  │      Flink Cluster      │  │
│  │  (Stateful) │  │  (Stateful) │  │  ┌───────────────────┐  │  │
│  │             │  │             │  │  │ JobManager        │  │  │
│  └─────────────┘  └─────────────┘  │  └───────────────────┘  │  │
│                                     │  ┌───────────────────┐  │  │
│  ┌─────────────┐  ┌─────────────┐  │  │ TaskManager (xN)  │  │  │
│  │   MySQL     │  │    Doris    │  │  └───────────────────┘  │  │
│  │  (Stateful) │  │  (Stateful) │  └─────────────────────────┘  │
│  └─────────────┘  └─────────────┘                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              Flink Application Deployments                   ││
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌───────────┐ ││
│  │  │  DIM   │ │ DWD-DB │ │DWD-LOG │ │  DWS   │ │ Publisher │ ││
│  │  └────────┘ └────────┘ └────────┘ └────────┘ └───────────┘ ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  ConfigMap  │  │   Secret    │  │    Prometheus + Grafana │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 组件说明

| 组件               | 类型        | 说明                       |
| ------------------ | ----------- | -------------------------- |
| Flink JobManager   | Deployment  | Flink 任务调度管理         |
| Flink TaskManager  | Deployment  | Flink 任务执行节点         |
| Kafka              | StatefulSet | 消息队列，数据缓冲         |
| MySQL              | StatefulSet | 业务数据库 + 配置库        |
| HBase              | StatefulSet | 维度数据存储               |
| Doris              | StatefulSet | OLAP 分析引擎              |
| Flink Applications | Deployment  | 各层数据处理任务           |
| Publisher Service  | Deployment  | 数据发布服务 (Spring Boot) |

## 二、前置要求

### 2.1 环境要求

- Kubernetes 集群版本 >= 1.19
- kubectl 命令行工具已配置
- Helm 3.x (可选，用于简化部署)
- 存储类 (StorageClass) 已配置，支持动态 PV 分配
- 镜像仓库 (可使用 Docker Hub 或私有仓库)

### 2.2 资源要求

| 组件               | CPU      | 内存   | 存储   |
| ------------------ | -------- | ------ | ------ |
| Flink JobManager   | 2 Core   | 4 Gi   | -      |
| Flink TaskManager  | 4 Core   | 8 Gi   | -      |
| Kafka (per broker) | 2 Core   | 4 Gi   | 50 Gi  |
| MySQL              | 2 Core   | 4 Gi   | 100 Gi |
| HBase Master       | 2 Core   | 4 Gi   | -      |
| HBase RegionServer | 4 Core   | 8 Gi   | 200 Gi |
| Doris FE           | 2 Core   | 4 Gi   | 50 Gi  |
| Doris BE           | 4 Core   | 16 Gi  | 500 Gi |
| Flink Applications | 2-4 Core | 4-8 Gi | -      |
| Publisher          | 1 Core   | 2 Gi   | -      |

## 三、部署步骤

### 3.1 准备阶段

#### Step 1: 创建命名空间

```bash
kubectl create namespace gmall-realtime
```

#### Step 2: 构建并推送 Docker 镜像

**基础 Flink 镜像 (包含项目依赖):**

```dockerfile
FROM flink:1.17.1-scala_2.12-java8

# 复制项目 JAR 包
COPY realtime-dim/target/realtime-dim-1.0-SNAPSHOT.jar /opt/flink/usrlib/
COPY realtime-dwd/*/target/*.jar /opt/flink/usrlib/
COPY realtime-dws/*/target/*.jar /opt/flink/usrlib/

# 复制配置文件
COPY config/log4j.properties /opt/flink/conf/
```

**Publisher 镜像:**

```dockerfile
FROM openjdk:8-jre-slim

WORKDIR /app
COPY gmall2024-publisher/target/gmall2024-publisher.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

#### Step 3: 推送镜像到仓库

```bash
docker build -t your-registry/gmall-flink:1.0 .
docker push your-registry/gmall-flink:1.0

docker build -t your-registry/gmall-publisher:1.0 -f Dockerfile.publisher .
docker push your-registry/gmall-publisher:1.0
```

### 3.2 部署外部依赖

#### 方式一：使用已有外部服务（推荐生产环境）

修改 ConfigMap 中的连接信息指向现有服务。

#### 方式二：在 K8s 内部署（适合测试/开发）

```bash
# 部署 Kafka (使用 Helm)
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install kafka bitnami/kafka -n gmall-realtime -f kafka-values.yaml

# 部署 MySQL
helm install mysql bitnami/mysql -n gmall-realtime -f mysql-values.yaml

# 部署 HBase (需要自定义 Chart 或使用 Operator)
# 部署 Doris (使用 Doris Operator 或手动部署)
```

### 3.3 部署 Flink 应用

#### Step 1: 应用所有资源配置

```bash
cd /workspace/k8s

# 应用 ConfigMap 和 Secret
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml

# 应用 Flink 部署
kubectl apply -f flink-deployment.yaml

# 应用各 Flink 任务
kubectl apply -f flink-dim-app.yaml
kubectl apply -f flink-dwd-base-db.yaml
kubectl apply -f flink-dwd-base-log.yaml
kubectl apply -f flink-dwd-trade-order-detail.yaml
kubectl apply -f flink-dws-user-login.yaml
kubectl apply -f flink-dws-trade-cart-add.yaml

# 应用 Publisher 服务
kubectl apply -f publisher-deployment.yaml
kubectl apply -f publisher-service.yaml
```

#### Step 2: 验证部署状态

```bash
# 查看所有 Pod 状态
kubectl get pods -n gmall-realtime

# 查看服务状态
kubectl get services -n gmall-realtime

# 查看日志
kubectl logs -f deployment/flink-dim-app -n gmall-realtime
```

### 3.4 提交 Flink 任务

如果使用 Flink Native Kubernetes 模式:

```bash
# 提交 DIM 应用
flink run-application \
  -t kubernetes-application \
  -Dkubernetes.cluster-id=dim-app-cluster \
  -Dkubernetes.namespace=gmall-realtime \
  -Dkubernetes.container.image=your-registry/gmall-flink:1.0 \
  -Dtaskmanager.memory.process.size=4096m \
  local:///opt/flink/usrlib/realtime-dim-1.0-SNAPSHOT.jar

# 提交 DWD 应用
flink run-application \
  -t kubernetes-application \
  -Dkubernetes.cluster-id=dwd-base-db-cluster \
  -Dkubernetes.namespace=gmall-realtime \
  -Dkubernetes.container.image=your-registry/gmall-flink:1.0 \
  local:///opt/flink/usrlib/realtime-dwd-base-db-1.0-SNAPSHOT.jar
```

## 四、配置说明

### 4.1 环境变量配置

所有 Flink 应用共享以下环境变量:

| 变量名                  | 说明                 | 来源      |
| ----------------------- | -------------------- | --------- |
| KAFKA_BOOTSTRAP_SERVERS | Kafka 连接地址       | ConfigMap |
| MYSQL_HOST              | MySQL 主机地址       | ConfigMap |
| MYSQL_PORT              | MySQL 端口           | ConfigMap |
| MYSQL_USERNAME          | MySQL 用户名         | Secret    |
| MYSQL_PASSWORD          | MySQL 密码           | Secret    |
| HBASE_ZOOKEEPER_QUORUM  | HBase ZooKeeper 地址 | ConfigMap |
| DORIS_FENODES           | Doris FE 节点地址    | ConfigMap |
| REDIS_HOST              | Redis 主机地址       | ConfigMap |

### 4.2 Checkpoint 配置

```yaml
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 600000
state.backend: rocksdb
state.checkpoints.dir: s3://your-bucket/checkpoints/
state.savepoints.dir: s3://your-bucket/savepoints/
```

## 五、运维管理

### 5.1 扩缩容

```bash
# 扩展 TaskManager 数量
kubectl scale deployment flink-taskmanager --replicas=10 -n gmall-realtime

# 扩展特定 Flink 应用
kubectl scale deployment flink-dim-app --replicas=3 -n gmall-realtime
```

### 5.2 更新应用

```bash
# 更新镜像版本
kubectl set image deployment/flink-dim-app \
  flink-dim-app=your-registry/gmall-flink:1.1 -n gmall-realtime

# 滚动更新状态
kubectl rollout status deployment/flink-dim-app -n gmall-realtime
```

### 5.3 故障恢复

```bash
# 从 Savepoint 恢复
flink run-application \
  -t kubernetes-application \
  -Dkubernetes.cluster-id=dim-app-cluster \
  -Dexecution.savepoint.path=s3://your-bucket/savepoints/savepoint-xxx \
  local:///opt/flink/usrlib/realtime-dim-1.0-SNAPSHOT.jar
```

### 5.4 日志收集

建议集成 EFK (Elasticsearch + Fluentd + Kibana) 或 Loki + Promtail 进行日志收集:

```bash
kubectl logs -f deployment/flink-dim-app -n gmall-realtime > dim-app.log
```

## 六、监控告警

### 6.1 Prometheus 监控

部署 Prometheus 抓取 Flink 指标:

```yaml
# prometheus-config.yaml
scrape_configs:
  - job_name: 'flink'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: flink.*
```

### 6.2 关键指标

- Job 状态 (running/failed/restarting)
- Checkpoint 成功率/延迟
- Task 处理延迟
- Backpressure 状态
- Kafka 消费 Lag
- 资源使用率 (CPU/Memory)

## 七、安全配置

### 7.1 RBAC 权限控制

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: flink-role
  namespace: gmall-realtime
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "delete"]
```

### 7.2 网络策略

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: flink-network-policy
  namespace: gmall-realtime
spec:
  podSelector:
    matchLabels:
      app: flink
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: flink
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: kafka
```

## 八、成本优化建议

1. **使用 Spot 实例**: 对于 TaskManager 等无状态组件，可使用 Spot 实例降低成本
2. **HPA 自动伸缩**: 基于 CPU/内存使用率自动调整副本数
3. **资源请求/限制**: 合理设置 requests/limits，避免资源浪费
4. **镜像优化**: 减小镜像体积，加快启动速度
5. **日志轮转**: 配置日志大小限制和保留策略

## 九、常见问题

### Q1: Flink 任务无法连接 Kafka?

检查 Kafka Service 是否正确配置，确保网络策略允许访问。

### Q2: Checkpoint 失败?

检查状态后端存储配置，确保有足够的存储空间和网络带宽。

### Q3: 数据倾斜如何处理?

在 Flink 代码中使用 Rebalance 或自定义分区器，或在 K8s 层面增加 TaskManager 数量。

### Q4: 如何优雅关闭 Flink 任务?

```bash
kubectl delete deployment flink-dim-app -n gmall-realtime
# Flink 会触发 Savepoint 并优雅关闭
```

## 十、参考文档

- [Flink Kubernetes 官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/resource-providers/native_kubernetes/)
- [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
- [Kafka Helm Chart](https://github.com/bitnami/charts/tree/main/bitnami/kafka)
- [Doris Kubernetes 部署](https://doris.apache.org/docs/admin-manual/deploy/cluster/kubernetes/)
