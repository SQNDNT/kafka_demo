package com.guihx.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MyProducer {

    private final static String TOPIC_NAME="test";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //键值对序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        
//        int msgNum = 5;
//        final CountDownLatch countDownLatch = new CountDownLatch(msgNum);
//        for (int i = 0; i < 100; i++) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(TOPIC_NAME, JSON.toJSONString("fack baby for every"));
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e!=null){
                        System.out.println("发送失败");
                    }
                    if(recordMetadata!=null){
                        System.out.println(recordMetadata.topic());
                        System.out.println(recordMetadata.partition());
                        System.out.println(recordMetadata.offset());
                    }
//                    countDownLatch.countDown();
                }
            });
//        }
//        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.close();
    }
}
