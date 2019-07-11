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
  * Created by SunYang on 2019/7/10 16:51
  */
object DataBaseLogETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataETL")
    // 得到一个SparkSession对象 ， 并实现对Hive的支持
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    val path = "F:\\HadoopProject\\data-warehouse-spark\\data-wareshouse-etl\\src\\main\\resources\\baseadlog.log"
    val memberSource: DataFrame = session.read.json(path)
    memberSource.show()
    val memberRDD: RDD[BaseLog] = memberSource.as[BaseLog].rdd
    session.sql("use bdl")
    session.sql("set hive.exec.dynamic.partition=true")
    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    memberRDD.mapPartitions{
      partition=>{
        partition.map{
          data=>{
            val time: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            BaseLogSql(data.adid , data.adname ,data.dn)
          }
        }
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_base_ad")
  }
}

case class BaseLog(
                  var adid:String,
                  var adname:String,
                  var dn :String
                  ){

}
case class BaseLogSql(
                    var adid:String,
                    var adname:String,
                    var dn :String
                  ){

}
