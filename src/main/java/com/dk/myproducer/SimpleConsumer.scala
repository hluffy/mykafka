package com.dk.myproducer

import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}


/**
  * 消费者
  */
object SimpleConsumer {
    def main(args: Array[String]): Unit = {
        val props = new Properties

        props.put("bootstrap.servers", "localhost:9092");
        //每个消费者分配独立的组号
        props.put("group.id", "test");

        //如果value合法，则自动提交偏移量
        props.put("enable.auto.commit", "true");

        //设置多久一次更新被消费消息的偏移量
        props.put("auto.commit.interval.ms", "1000");

        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        props.put("session.timeout.ms", "30000");

        props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");


        val consumer = new KafkaConsumer[String,String](props)

        consumer.subscribe(Collections.singletonList("test"))

        while(true){
            val records = consumer.poll(100)

            records.forEach(record => {
                printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value())
            })


        }

    }

}
