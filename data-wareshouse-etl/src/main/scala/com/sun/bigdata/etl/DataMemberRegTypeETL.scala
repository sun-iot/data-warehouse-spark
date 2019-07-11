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
  * Created by SunYang on 2019/7/10 18:18
  */
object DataMemberRegTypeETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DataMemberRegType").setMaster("local[*]")
    // 得到一个SparkSession对象 ， 并实现对Hive的支持
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    val path = "F:\\HadoopProject\\data-warehouse-spark\\data-wareshouse-etl\\src\\main\\resources\\memberRegtype.log"
    val memberSource: DataFrame = session.read.json(path)
    memberSource.show()
    val DataMemberRegTypeRDD: RDD[DataMemberRegType] = memberSource.as[DataMemberRegType].rdd
    session.sql("use bdl")
    session.sql("set hive.exec.dynamic.partition=true")
    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    DataMemberRegTypeRDD.mapPartitions{
      partition=>{
       partition.map{
         data=>{
           val time = new SimpleDateFormat("yyyyMMdd").format(new Date())
            val resourcename = data.regsource match{
              case "1" => "PC"
              case "2" => "MOBILE"
              case "3"=> "APP"
              case "4" => "WECHAT"
              case _ => "OTHER"
            }
           DataMemberRegTypeSql(data.uid,data.appkey,data.appregurl,data.bdp_uuid,data.createtime,data.domain,data.isranreg,data.regsource,resourcename,data.websiteid,time,data.dn)
         }
       }
      }
    }.toDF().coalesce(2).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_member_regtype")
  }
}

case class DataMemberRegType(
                              var appkey: String,
                              var appregurl: String,
                              var bdp_uuid: String,
                              var createtime: String,
                              var dn: String,
                              var domain: String,
                              var isranreg: String,
                              var regsource: String,
                              var uid: String,
                              var websiteid: String
                            ) {}

case class DataMemberRegTypeSql(
                                 var uid: String,
                                 var appkey: String,
                                 var appregurl: String,
                                 var bdp_uuid: String,
                                 var createtime: String,
                                 var domain: String,
                                 var isranreg: String,
                                 var regsource: String,
                                 var regsourcename: String,
                                 var websiteid: String,
                                 var time :String,
                                 var dn: String
                               ) {}
