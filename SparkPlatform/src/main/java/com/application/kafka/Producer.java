package com.application.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author 张睿
 * @create 2020-05-18 9:46
 * kafka消息生产者
 **/
public class Producer {

    public static void main(String[] args) {
        String topic = "test";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.54:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        try {
            String key = "hello";
            String mes = "hello002";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,mes);
            Future<RecordMetadata> future =  kafkaProducer.send(record);
            System.out.println(future.get());
            System.out.println("消息发送成功");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}

