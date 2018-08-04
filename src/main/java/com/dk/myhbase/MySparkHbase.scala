package com.dk.myhbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark读hbase
  */
object MySparkHbase {
    def main(args: Array[String]): Unit = {
        val config = new SparkConf().setMaster("local[*]").setAppName("KafkaAndSpark")
        val sc = new SparkContext(config)

        val conf = HBaseConfiguration.create()
//        conf.set("hbase.zookeeper.property.clientPort", "2181")
//        conf.set("hbase.zookeeper.quorum", "master")

        //设置查询的表名
        conf.set(TableInputFormat.INPUT_TABLE, "test")

        val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])


        val count = usersRDD.count()
        println("Users RDD Count:" + count)
        usersRDD.cache()

        //遍历输出
        usersRDD.foreach{ case (_,result) =>
            val key = Bytes.toInt(result.getRow)
            val b = Bytes.toString(result.getValue("cf".getBytes,"b".getBytes))
            val name = Bytes.toString(result.value)
//            val age = Bytes.toInt(result.getValue("cf".getBytes,"b".getBytes))
//            println("Row key:"+key+" Name:"+name+" Age:"+age)
            println(key+":"+name+":"+b)
        }
        println("--------------------------------------------------")
        usersRDD.foreach(result => {
            val key = Bytes.toInt(result._2.getRow)
            val name = Bytes.toString(result._2.value)
            println(key+":"+name)

        })

    }

}
