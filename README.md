# kkmall-mq-consumer

独立 RocketMQ 消费服务，负责消费 Debezium CDC topic，并按处理器配置将消息投影到目标读模型。

## 主要职责

- 基于配置表订阅 RocketMQ topic
- 解析 Debezium CDC 消息
- 将主数据写入 Redis 主缓存
- 维护 Redis 二级索引缓存
- 支持后续扩展到 MySQL、ES、统计等其他 handler

## 配置表

请执行 `src/main/resources/sql/cdc_config_tables.sql` 初始化：

- `mq_subscription_config`：定义订阅哪些 topic
- `mq_handler_config`：定义收到消息后如何处理

## 当前已实现的 handler

- `REDIS_CACHE`

`REDIS_CACHE` 的 `config_json` 示例：

```json
{
  "logicalTable": "mall_product",
  "pkField": "id",
  "ttlSeconds": null,
  "indexes": [
    { "field": "product_code", "type": "UNIQUE" },
    { "field": "category_id", "type": "SET" }
  ]
}
```

## `config_json` 字段说明

当前 `REDIS_CACHE` handler 已支持以下字段：

### `logicalTable`

- 作用：指定写入 Redis 时使用的逻辑表名
- 是否必填：是
- 示例：`mall_product`
- 影响的 Redis key：
  - 主记录：`cdc:{env}:{logicalTable}:{id}`
  - 二级索引：`cdc_idx:{env}:{logicalTable}:{field}:{value}`

### `pkField`

- 作用：指定主键字段名，用于从 CDC 消息中取出 Redis 主记录的 `{id}`
- 是否必填：否
- 默认值：`id`
- 示例：`id`

### `dataKeyPrefix`

- 作用：指定主记录 Redis key 前缀
- 是否必填：否
- 默认值：走全局配置 `kkmall.mq-consumer.cache.data-key-prefix`，当前默认 `cdc`
- 示例：`cdc`

### `indexKeyPrefix`

- 作用：指定二级索引 Redis key 前缀
- 是否必填：否
- 默认值：走全局配置 `kkmall.mq-consumer.cache.index-key-prefix`，当前默认 `cdc_idx`
- 示例：`cdc_idx`

### `ttlSeconds`

- 作用：指定主记录和索引 key 的过期时间，单位秒
- 是否必填：否
- 默认行为：`null` 或 `<= 0` 表示不过期
- 示例：`86400`

### `indexes`

- 作用：定义 Redis 二级索引列表
- 是否必填：否
- 支持类型：
  - `UNIQUE`：一个值对应一条记录
  - `SET`：一个值对应多条记录
- 示例：

```json
[
  { "field": "product_code", "type": "UNIQUE" },
  { "field": "category_id", "type": "SET" }
]
```

## `indexes` 字段说明

每个索引项包含两个字段：

### `field`

- 作用：指定要建索引的字段名
- 注意：必须写 **数据库列名**，不要写 Java 字段名
- 正确示例：`order_no`、`user_id`、`product_code`、`category_id`
- 错误示例：`orderNo`、`userId`、`productCode`、`categoryId`

### `type`

- `UNIQUE`：唯一索引，例如 `order_no`、`product_code`、`phone`
- `SET`：集合索引，例如 `user_id`、`category_id`、`order_id`

## 关键 Redis Key

- 主记录：`cdc:{env}:{logicalTable}:{id}`
- 二级索引：`cdc_idx:{env}:{logicalTable}:{field}:{value}`

例如：

- 主记录：`cdc:dev:mall_product:101`
- 唯一索引：`cdc_idx:dev:mall_product:product_code:IP15PMX -> 101`
- 集合索引：`cdc_idx:dev:mall_product:category_id:5 -> Set(101,102,103)`

## topic 配置示例

以 `kkmall_kkmall-dev_mall_product` 为例：

```sql
INSERT INTO mq_subscription_config
(env, topic, consumer_group, message_type, source_system, source_db, source_table, parser_type, enabled, remark)
VALUES
('dev', 'kkmall_kkmall-dev_mall_product', 'kkmall-mq-consumer-dev', 'CDC', 'DEBEZIUM', 'kkmall-dev', 'mall_product', 'CDC_DEBEZIUM', 1, '商品CDC');
```

```sql
INSERT INTO mq_handler_config
(subscription_id, handler_type, handler_name, config_json, enabled, sort_order, remark)
VALUES
(
  1,
  'REDIS_CACHE',
  'product-cache',
  JSON_OBJECT(
    'logicalTable', 'mall_product',
    'pkField', 'id',
    'indexes', JSON_ARRAY(
      JSON_OBJECT('field', 'product_code', 'type', 'UNIQUE'),
      JSON_OBJECT('field', 'category_id', 'type', 'SET')
    )
  ),
  1,
  1,
  '商品缓存处理'
);
```

## 说明

- 商品点击统计不在本服务中处理，由 `kkmall-mall-api` 直接写 Redis。
- 当前服务只处理 `message_type = CDC` 且存在 `REDIS_CACHE` handler 的订阅配置。
- 如果后续新增其他 handler，例如 `MYSQL_SINK`、`ES_INDEX`、`PROFILE_TAG`，建议使用各自独立的 `config_json` 结构，不要复用 `REDIS_CACHE` 的字段定义。

## RocketMQ 连接模式

- 默认配置用于集群外 / Tailscale 访问：`10.0.0.166:9876`
- K3s 集群内运行时，设置 `SPRING_PROFILES_ACTIVE=k8s`，默认改为 `rocketmq-nameserver.rocketmq.svc.cluster.local:9876`
- 如需显式覆盖，统一使用环境变量 `KKMALL_MQ_CONSUMER_NAMESRV_ADDR`
