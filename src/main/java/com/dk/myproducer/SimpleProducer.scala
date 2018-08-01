package com.dk.myproducer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

object SimpleProducer {
    def main(args: Array[String]): Unit = {
        val pop = new Properties
        pop.put("bootstrap.servers","localhost:9092")
        pop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
        pop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
//        pop.put("metadata.fetch.timeout.ms","5000")
        val producer = new KafkaProducer[String,String](pop)

        val record = new ProducerRecord[String,String]("test","Hello World from Mac!")

        producer.send(record)
        println("over")

    }

}
