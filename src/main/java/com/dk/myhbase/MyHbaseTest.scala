package com.dk.myhbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * scala操作hbase
  */
class MyHbaseTest {

}

object MyHbaseTest {
    def main(args: Array[String]): Unit = {
        val conf = HBaseConfiguration.create()

        val conn = ConnectionFactory.createConnection(conf)
        val admin = conn.getAdmin

        admin.listTables().foreach(table => println(table.getTableName))

        val table = new HTable(conf,"test")
        println(table)

//        添加数据
        val theput = new Put(Bytes.toBytes("row4"))
        theput.add(Bytes.toBytes("cf"),Bytes.toBytes("d"),Bytes.toBytes("value4"))
        table.put(theput)

//        获取数据
        val theget = new Get(Bytes.toBytes("row4"))
        val result = table.get(theget)
        println(Bytes.toString(result.value))
    }

}
