package com.company.project.configurer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ配置类
 */
@Configuration
@ConfigurationProperties(prefix = "rocketmq")
public class RocketMQConfigurer {
    
    /**
     * NameServer地址
     */
    private String namesrvAddr = "127.0.0.1:9876";
    
    /**
     * 生产者组名
     */
    private String producerGroup = "transaction_producer_group";
    
    /**
     * 消费者组名
     */
    private String consumerGroup = "transaction_consumer_group";
    
    /**
     * 事务消息主题
     */
    private String transactionTopic = "TransactionMessage";
    
    /**
     * 消息标签
     */
    private String messageTag = "*";

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTransactionTopic() {
        return transactionTopic;
    }

    public void setTransactionTopic(String transactionTopic) {
        this.transactionTopic = transactionTopic;
    }

    public String getMessageTag() {
        return messageTag;
    }

    public void setMessageTag(String messageTag) {
        this.messageTag = messageTag;
    }
}

