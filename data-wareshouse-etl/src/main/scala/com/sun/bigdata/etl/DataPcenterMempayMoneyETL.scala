package com.sun.bigdata.etl

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: data-warehouse-spark
  * Package: com.sun.bigdata.etl
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/10 18:07
  */
object DataPcenterMempayMoneyETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DataPcenterMempayMoneyETL").setMaster("local[*]")
    // 得到一个SparkSession对象 ， 并实现对Hive的支持
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    val path = "F:\\HadoopProject\\data-warehouse-spark\\data-wareshouse-etl\\src\\main\\resources\\pcentermempaymoney.log"
    val memberSource: DataFrame = session.read.json(path)
    memberSource.show()
    val memberRDD: RDD[DataPcenterMempayMoney] = memberSource.as[DataPcenterMempayMoney].rdd
    session.sql("use bdl")
    session.sql("set hive.exec.dynamic.partition=true")
    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    memberRDD.mapPartitions {
      partition => {
        partition.map {
          data => {
            val time: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            DataPcenterMempayMoneySql(data.uid,data.paymoney,data.siteid,data.vip_id,time,data.dn)
          }
        }
      }
    }.toDF().coalesce(2).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_pcentermempaymoney")
  }
}

case class DataPcenterMempayMoney(
                                   var dn: String,
                                   var paymoney: String,
                                   var siteid: String,
                                   var uid: String,
                                   var vip_id: String
                                 ) {}

case class DataPcenterMempayMoneySql(
                                      var uid: String,
                                      var paymoney: String,
                                      var siteid: String,
                                      var vip_id: String,
                                      var time :String,
                                      var dn: String
                                    ) {}