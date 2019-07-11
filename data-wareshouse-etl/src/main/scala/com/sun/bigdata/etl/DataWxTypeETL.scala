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
  * bdl_wxtype
  * Created by SunYang on 2019/7/10 16:30
  */
object DataWxTypeETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataETL")
    // 得到一个SparkSession对象 ， 并实现对Hive的支持
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    val path = "F:\\HadoopProject\\data-warehouse-spark\\data-wareshouse-etl\\src\\main\\resources\\WxType.log"
    val memberSource: DataFrame = session.read.json(path)
    memberSource.show()
    val memberRDD: RDD[Wxtype] = memberSource.as[Wxtype].rdd
    session.sql("use bdl")
    session.sql("set hive.exec.dynamic.partition=true")
    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    memberRDD.mapPartitions{
      partition=>{
        partition.map{
          data=>{
              val time: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            WxtypsSql(data.bind_source,data.app_id,data.token_secret,data.wx_name,data.wx_no,data.createtor,data.createtime,data.dn)
          }
        }
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_wxtype")
  }
}

case class Wxtype(
                 var app_id:String,
                 var bind_source: String,
                 var createtime:String,
                 var createtor:String,
                 var dn :String ,
                 var token_secret :String,
                 var wx_name : String ,
                 var wx_no : String
                 ){

}

case class WxtypsSql(
                    var bind_source:String,
                    var app_id:String,
                    var token_secret:String,
                    var wx_name : String ,
                    var wx_no : String,
                    var createtor:String,
                    var createtime:String,
                    var website:String
                    ){

}
