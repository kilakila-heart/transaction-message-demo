/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.company.project.biz;

import com.company.project.biz.service.ConsumerService;
import com.company.project.configurer.RocketMQConfigurer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * RocketMQ事务消息消费者
 * 集成到Spring Boot中作为Bean
 */
@Component
public class Consumer implements InitializingBean, DisposableBean {

    private DefaultMQPushConsumer consumer;
    
    @Resource
    private ConsumerService consumerService;
    
    @Autowired
    private RocketMQConfigurer rocketMQConfigurer;

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            // 创建消费者实例
            consumer = new DefaultMQPushConsumer(rocketMQConfigurer.getConsumerGroup());
            
            // 设置NameServer地址
            consumer.setNamesrvAddr(rocketMQConfigurer.getNamesrvAddr());
            
            // 设置消费起始位置
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            
            // 订阅主题
            consumer.subscribe(rocketMQConfigurer.getTransactionTopic(), rocketMQConfigurer.getMessageTag());
            
            // 注册消息监听器
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        try {
                            String messageBody = new String(msg.getBody());
                            System.out.println("=== 收到事务消息 ===");
                            System.out.println("消息ID: " + msg.getMsgId());
                            System.out.println("消息内容: " + messageBody);
                            System.out.println("消息标签: " + msg.getTags());
                            System.out.println("消息主题: " + msg.getTopic());
                            System.out.println("==================");
                            
                            // 使用ConsumerService处理业务逻辑
                            boolean success = consumerService.processTransferMessage(messageBody);
                            
                            if (!success) {
                                System.err.println("=== 业务处理失败，将重试 ===");
                                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                            }

                        } catch (Exception e) {
                            System.err.println("消费消息时发生异常: " + e.getMessage());
                            e.printStackTrace();
                            // 返回重试状态，让RocketMQ重新投递消息
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            
            // 启动消费者
            consumer.start();
            System.out.println("=== RocketMQ消费者启动成功 ===");
            System.out.println("消费者组: " + rocketMQConfigurer.getConsumerGroup());
            System.out.println("订阅主题: " + rocketMQConfigurer.getTransactionTopic());
            System.out.println("NameServer: " + rocketMQConfigurer.getNamesrvAddr());
            System.out.println("================================");
            
        } catch (MQClientException e) {
            System.err.println("启动RocketMQ消费者失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("启动RocketMQ消费者失败", e);
        }
    }

    @Override
    public void destroy() throws Exception {
        if (consumer != null) {
            consumer.shutdown();
            System.out.println("=== RocketMQ消费者已关闭 ===");
        }
    }

    /**
     * 获取消费者实例（用于测试或其他用途）
     */
    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    /**
     * 检查消费者是否正在运行
     */
    public boolean isRunning() {
        return consumer != null && consumer.getDefaultMQPushConsumerImpl().getServiceState().name().equals("RUNNING");
    }
}
