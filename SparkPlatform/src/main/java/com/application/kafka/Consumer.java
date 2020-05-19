package com.application.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author 张睿
 * @create 2020-05-19 11:30
 * kafka消息消费者
 **/
public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.54:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        /**GROUP_ID在每次消费者长连接（轮询端口）kafka后，需要更改GROUP_ID名称*/
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group003");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        /**为消费者分配对应分区partition，有两种模式
         * subscribe(自动)：为消费者自动分配partition
         * assign(手动)：消费者手动指定需要消费的partition*/
        kafkaConsumer.subscribe(Collections.singletonList("test"));

        TopicPartition partition = new TopicPartition("topic1",0);
        kafkaConsumer.assign(Collections.singleton(partition));

        /**consumer.poll(long timeout)方法：
         * timeout 单次poll最大等待时间，缓存中数据被消费结束后，consumer会去kafka消息持久化broker中拉取数据
         * 最多等待时间就是timeout参数，timeout越大，单次poll返回数据越多，如果timeout参数设置为1000，则是一次性
         * 拉取broker中所有数据*/
        while (true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofSeconds(1000));
            for (ConsumerRecord<String, String> record:records){
                System.out.println(record.partition());
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.offset());
            }
        }
    }
}

