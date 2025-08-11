package com.company.project.isolation;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 并发消费者（弱化队列级顺序，保证同订单内串行）：
 * - 使用 MessageListenerConcurrently 提高拉取与分发吞吐
 * - 使用 PerKeySerialExecutor 按 orderId 将任务路由到对应串行执行器
 * - 示例中演示核心思路；若需持久化幂等，可将处理记录落 MySQL/Redis
 */
public class OrderMsgConcurrentConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_concurrent_group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("TopicTest", "*");
        // 提升并发度（取决于机器与任务耗时）
        consumer.setConsumeThreadMin(8);
        consumer.setConsumeThreadMax(32);

        // 共享串行执行器：同一orderId串行，不同orderId并行
        PerKeySerialExecutor perKeyExecutor = new PerKeySerialExecutor(32, "order-worker-");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    // 解析订单ID
                    String orderId = msg.getUserProperty("orderId");
                    if (orderId == null) {
                        // 退化到消息Key或其他字段
                        orderId = msg.getKeys();
                    }
                    final String key = Optional.ofNullable(orderId).orElse("__unknown__");

                    // 将实际业务处理提交到对应的串行执行器
                    String body = new String(msg.getBody(), StandardCharsets.UTF_8);
                    final String msgId = msg.getMsgId();
                    perKeyExecutor.execute(key, () -> {
                        try {
                            // 模拟业务处理（可替换为调用Service，并做DB幂等，如MySQL基于唯一键插入）
                            System.out.printf("[order=%s] process msgId=%s body=%s on %s%n",
                                    key, msgId, body, Thread.currentThread().getName());
                            // 假设处理耗时
                            TimeUnit.MILLISECONDS.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Throwable t) {
                            // 记录日志并标记失败，依赖MQ重试（并保证幂等）
                            System.err.printf("[order=%s] process error for msgId=%s: %s%n", key, msgId, t.getMessage());
                            // 真实场景：将失败信息写入告警/重试表等
                        }
                    });
                }
                // 这里先返回成功，依赖业务侧幂等/重试策略（若要严格ACK，可改为延迟ACK模式）
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Concurrent Consumer Started.");

        // 可选：定时清理空key
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            consumer.shutdown();
        }));
    }
}
