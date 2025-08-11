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

import com.alibaba.fastjson.JSON;
import com.company.project.biz.entity.TransferRecord;
import com.company.project.configurer.RocketMQConfigurer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Component
public class TransactionProducer implements InitializingBean {
    private TransactionMQProducer producer;

    @Resource
    private TransactionListenerImpl transactionListener;
    
    @Autowired
    private RocketMQConfigurer rocketMQConfigurer;

    @Override
    public void afterPropertiesSet() throws Exception {
        producer = new TransactionMQProducer(rocketMQConfigurer.getProducerGroup());
        producer.setNamesrvAddr(rocketMQConfigurer.getNamesrvAddr());

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        producer.setExecutorService(executorService);
        //设置回调检查监听器
        producer.setTransactionListener(transactionListener);
        try {
            producer.start();
            System.out.println("=== RocketMQ事务消息生产者启动成功 ===");
            System.out.println("生产者组: " + rocketMQConfigurer.getProducerGroup());
            System.out.println("NameServer: " + rocketMQConfigurer.getNamesrvAddr());
            System.out.println("================================");
        } catch (MQClientException e) {
            System.err.println("启动RocketMQ事务消息生产者失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("启动RocketMQ事务消息生产者失败", e);
        }
    }

    public void test() {
        //单次转账唯一编号
        String businessNo = UUID.randomUUID().toString();

        //要发送的事务消息 设置转账人 被转账人 转账金额
        TransferRecord transferRecord = new TransferRecord();
        transferRecord.setFromUserId(1L);
        transferRecord.setToUserId(2L);
        transferRecord.setChangeMoney(100L);
        transferRecord.setRecordNo(businessNo);

        try {
            Message msg = new Message(rocketMQConfigurer.getTransactionTopic(), rocketMQConfigurer.getMessageTag(), businessNo,
                    JSON.toJSONString(transferRecord).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            System.out.println("prepare事务消息发送结果:"+sendResult.getSendStatus());
        } catch (Exception e) {
            System.err.println("发送事务消息失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
