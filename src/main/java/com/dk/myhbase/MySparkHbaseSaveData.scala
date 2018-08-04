package com.dk.myhbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark操作hbase
  * 保存数据
  */
object MySparkHbaseSaveData {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("mySparkHbaseSaveData")
        val sc = new SparkContext(conf)

        val data = sc.parallelize(Array("1,jack,15","2,Lily,16","3,mike,16"))
//

        data.foreachPartition(arr => {
            val config = HBaseConfiguration.create()
            val conn = ConnectionFactory.createConnection(config)
            val table = conn.getTable(TableName.valueOf("user"))

            val list = new java.util.ArrayList[Put]
            arr.foreach(result => {
                val array = result.split(",")
                val put = new Put(Bytes.toBytes(array(0).toInt))

                put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(array(1).toString))
                put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(array(2).toString))
                list.add(put)
            })
            table.put(list)
            table.close()
        })

        println("over!")



    }

}
