CREATE TABLE IF NOT EXISTS mq_subscription_config (
  id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '订阅配置ID',
  env VARCHAR(32) NOT NULL COMMENT '环境标识，例如 dev/test/prod',
  topic VARCHAR(255) NOT NULL COMMENT 'RocketMQ Topic 名称',
  consumer_group VARCHAR(128) NOT NULL COMMENT 'RocketMQ 消费组',
  message_type VARCHAR(64) NOT NULL COMMENT '消息类型，例如 CDC、BEHAVIOR',
  source_system VARCHAR(64) NULL COMMENT '消息来源系统，例如 DEBEZIUM',
  source_db VARCHAR(128) NULL COMMENT '来源数据库名称',
  source_table VARCHAR(128) NULL COMMENT '来源表名称',
  parser_type VARCHAR(64) NOT NULL DEFAULT 'CDC_DEBEZIUM' COMMENT '消息解析器类型，例如 CDC_DEBEZIUM',
  enabled TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用：1 启用，0 禁用',
  remark VARCHAR(255) NULL COMMENT '备注说明',
  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  UNIQUE KEY uk_env_topic_group (env, topic, consumer_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='MQ 订阅配置表：定义消费哪些 Topic 以及如何识别消息来源';

CREATE TABLE IF NOT EXISTS mq_handler_config (
  id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '处理器配置ID',
  subscription_id BIGINT NOT NULL COMMENT '关联 mq_subscription_config.id',
  handler_type VARCHAR(64) NOT NULL COMMENT '处理器类型，例如 REDIS_CACHE、MYSQL_SINK、ES_INDEX',
  handler_name VARCHAR(128) NOT NULL COMMENT '处理器名称，用于区分同类型的不同处理逻辑',
  config_json JSON NOT NULL COMMENT '处理器配置参数，JSON 格式',
  enabled TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用：1 启用，0 禁用',
  sort_order INT NOT NULL DEFAULT 0 COMMENT '同一订阅下处理器执行顺序，值越小越先执行',
  remark VARCHAR(255) NULL COMMENT '备注说明',
  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  KEY idx_subscription_id (subscription_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='MQ 处理器配置表：定义订阅到消息后要执行哪些处理动作';

-- ------------------------------------------------------------
-- 示例初始化：dev 环境下的 4 个 Debezium CDC Topic
-- 可直接执行；已使用 NOT EXISTS 保证重复执行时不报重复插入
-- ------------------------------------------------------------

INSERT INTO mq_subscription_config
(env, topic, consumer_group, message_type, source_system, source_db, source_table, parser_type, enabled, remark)
SELECT
  'dev',
  'kkmall_kkmall-dev_mall_order',
  'kkmall-mq-consumer-dev',
  'CDC',
  'DEBEZIUM',
  'kkmall-dev',
  'mall_order',
  'CDC_DEBEZIUM',
  1,
  '订单主表'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1 FROM mq_subscription_config
  WHERE env = 'dev'
    AND topic = 'kkmall_kkmall-dev_mall_order'
    AND consumer_group = 'kkmall-mq-consumer-dev'
);

INSERT INTO mq_subscription_config
(env, topic, consumer_group, message_type, source_system, source_db, source_table, parser_type, enabled, remark)
SELECT
  'dev',
  'kkmall_kkmall-dev_mall_product',
  'kkmall-mq-consumer-dev',
  'CDC',
  'DEBEZIUM',
  'kkmall-dev',
  'mall_product',
  'CDC_DEBEZIUM',
  1,
  '商品主表'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1 FROM mq_subscription_config
  WHERE env = 'dev'
    AND topic = 'kkmall_kkmall-dev_mall_product'
    AND consumer_group = 'kkmall-mq-consumer-dev'
);

INSERT INTO mq_subscription_config
(env, topic, consumer_group, message_type, source_system, source_db, source_table, parser_type, enabled, remark)
SELECT
  'dev',
  'kkmall_kkmall-dev_mall_user',
  'kkmall-mq-consumer-dev',
  'CDC',
  'DEBEZIUM',
  'kkmall-dev',
  'mall_user',
  'CDC_DEBEZIUM',
  1,
  '用户主表'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1 FROM mq_subscription_config
  WHERE env = 'dev'
    AND topic = 'kkmall_kkmall-dev_mall_user'
    AND consumer_group = 'kkmall-mq-consumer-dev'
);

INSERT INTO mq_subscription_config
(env, topic, consumer_group, message_type, source_system, source_db, source_table, parser_type, enabled, remark)
SELECT
  'dev',
  'kkmall_kkmall-dev_mall_order_item',
  'kkmall-mq-consumer-dev',
  'CDC',
  'DEBEZIUM',
  'kkmall-dev',
  'mall_order_item',
  'CDC_DEBEZIUM',
  1,
  '订单明细表'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1 FROM mq_subscription_config
  WHERE env = 'dev'
    AND topic = 'kkmall_kkmall-dev_mall_order_item'
    AND consumer_group = 'kkmall-mq-consumer-dev'
);

INSERT INTO mq_handler_config
(subscription_id, handler_type, handler_name, config_json, enabled, sort_order, remark)
SELECT
  s.id,
  'REDIS_CACHE',
  'order-cache',
  JSON_OBJECT(
    'logicalTable', 'mall_order',
    'pkField', 'id',
    'indexes', JSON_ARRAY(
      JSON_OBJECT('field', 'order_no', 'type', 'UNIQUE'),
      JSON_OBJECT('field', 'user_id', 'type', 'SET')
    )
  ),
  1,
  1,
  '订单缓存处理'
FROM mq_subscription_config s
WHERE s.env = 'dev'
  AND s.topic = 'kkmall_kkmall-dev_mall_order'
  AND s.consumer_group = 'kkmall-mq-consumer-dev'
  AND NOT EXISTS (
    SELECT 1 FROM mq_handler_config h
    WHERE h.subscription_id = s.id
      AND h.handler_type = 'REDIS_CACHE'
      AND h.handler_name = 'order-cache'
  );

INSERT INTO mq_handler_config
(subscription_id, handler_type, handler_name, config_json, enabled, sort_order, remark)
SELECT
  s.id,
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
FROM mq_subscription_config s
WHERE s.env = 'dev'
  AND s.topic = 'kkmall_kkmall-dev_mall_product'
  AND s.consumer_group = 'kkmall-mq-consumer-dev'
  AND NOT EXISTS (
    SELECT 1 FROM mq_handler_config h
    WHERE h.subscription_id = s.id
      AND h.handler_type = 'REDIS_CACHE'
      AND h.handler_name = 'product-cache'
  );

INSERT INTO mq_handler_config
(subscription_id, handler_type, handler_name, config_json, enabled, sort_order, remark)
SELECT
  s.id,
  'REDIS_CACHE',
  'user-cache',
  JSON_OBJECT(
    'logicalTable', 'mall_user',
    'pkField', 'id',
    'indexes', JSON_ARRAY(
      JSON_OBJECT('field', 'username', 'type', 'UNIQUE'),
      JSON_OBJECT('field', 'phone', 'type', 'UNIQUE')
    )
  ),
  1,
  1,
  '用户缓存处理'
FROM mq_subscription_config s
WHERE s.env = 'dev'
  AND s.topic = 'kkmall_kkmall-dev_mall_user'
  AND s.consumer_group = 'kkmall-mq-consumer-dev'
  AND NOT EXISTS (
    SELECT 1 FROM mq_handler_config h
    WHERE h.subscription_id = s.id
      AND h.handler_type = 'REDIS_CACHE'
      AND h.handler_name = 'user-cache'
  );

INSERT INTO mq_handler_config
(subscription_id, handler_type, handler_name, config_json, enabled, sort_order, remark)
SELECT
  s.id,
  'REDIS_CACHE',
  'order-item-cache',
  JSON_OBJECT(
    'logicalTable', 'mall_order_item',
    'pkField', 'id',
    'indexes', JSON_ARRAY(
      JSON_OBJECT('field', 'order_id', 'type', 'SET'),
      JSON_OBJECT('field', 'product_id', 'type', 'SET')
    )
  ),
  1,
  1,
  '订单明细缓存处理'
FROM mq_subscription_config s
WHERE s.env = 'dev'
  AND s.topic = 'kkmall_kkmall-dev_mall_order_item'
  AND s.consumer_group = 'kkmall-mq-consumer-dev'
  AND NOT EXISTS (
    SELECT 1 FROM mq_handler_config h
    WHERE h.subscription_id = s.id
      AND h.handler_type = 'REDIS_CACHE'
      AND h.handler_name = 'order-item-cache'
  );