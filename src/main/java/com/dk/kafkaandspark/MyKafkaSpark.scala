package com.dk.kafkaandspark

import java.util

import com.alibaba.fastjson.JSON
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/**
  * kafka and spark
  */
object MyKafkaSpark {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaAndSpark")
        val ssc = new StreamingContext(conf,Seconds(2))


        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "use_a_separate_group_id_for_each_stream",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("test")
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )


//        stream.map(record => (record.key, record.value)).print

        stream.map(record => record.value).print

        stream.foreachRDD(arr => {
            arr.foreachPartition(record => {
                val config = HBaseConfiguration.create()
                val conn = ConnectionFactory.createConnection(config)
                val table = conn.getTable(TableName.valueOf("users"))

                val list = new util.ArrayList[Put]
                record.foreach(result => {
                    val value = result.value()
                    val jsons = JSON.parseArray(value)
                    for(i <- 0 until jsons.size){
                        val json = jsons.getJSONObject(i)
                        val put = new Put(Bytes.toBytes(json.getString("id")))

                        put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),json.getString("name").getBytes())
                        put.add(Bytes.toBytes("cf"),Bytes.toBytes("birthday"),Bytes.toBytes(json.getString("birthday")))
                        put.add(Bytes.toBytes("cf"),Bytes.toBytes("telephone"),Bytes.toBytes(json.getString("telephone")))
                        put.add(Bytes.toBytes("cf"),Bytes.toBytes("password"),Bytes.toBytes(json.getString("password")))

                        list.add(put)
                    }
                })

                table.put(list)
                table.close()
//                println("save success!")
            })
        })

        println("over")

        ssc.start
        ssc.awaitTermination


    }

}
