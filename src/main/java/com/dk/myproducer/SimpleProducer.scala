package com.dk.myproducer


import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

object SimpleProducer {
    def main(args: Array[String]): Unit = {
        val pop = new Properties
        pop.put("bootstrap.servers","localhost:9092")
        pop.put("acks","all")
        pop.put("retries","0")
        pop.put("batch.size","16384")
        pop.put("linger.ms","1")
        pop.put("buffer.memory","33554432")
        pop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
        pop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String,String](pop)

        for(i <- 0 until 100){
            val record = new ProducerRecord[String,String]("test3","Hello World from Mac!-----"+i)
            println(i)
            producer.send(record)
        }
//        producer.flush
        producer.close

        println("over")

    }

}
