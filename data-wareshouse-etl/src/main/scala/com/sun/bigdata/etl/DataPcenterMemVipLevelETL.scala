package com.sun.bigdata.etl

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
  * Created by SunYang on 2019/7/10 18:42
  */
object DataPcenterMemVipLevelETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DataPcenterMemVipLevelETL").setMaster("local[*]")
    // 得到一个SparkSession对象 ， 并实现对Hive的支持
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    val path = "F:\\HadoopProject\\data-warehouse-spark\\data-wareshouse-etl\\src\\main\\resources\\PcenterMemViplevel.log"
    val memberSource: DataFrame = session.read.json(path)
    memberSource.show()
    val dataMemberVipRDD: RDD[DataPcenterMemVipLevel] = memberSource.as[DataPcenterMemVipLevel].rdd
    session.sql("use bdl")
    session.sql("set hive.exec.dynamic.partition=true")
    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    dataMemberVipRDD.mapPartitions {
      partition => {
        partition.map {
          data => {
            DataPcenterMemVipLevelSql(data.vip_id , data.vip_level , data.start_time,data.end_time,data.last_modify_time,data.max_free,data.min_free,data.next_level,data.operator,data.dn)
          }
        }
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_vip_level")
  }
}

case class DataPcenterMemVipLevel(
                                   discountval: String,
                                   dn: String,
                                   end_time: String,
                                   last_modify_time: String,
                                   max_free: String,
                                   min_free: String,
                                   next_level: String,
                                   operator: String,
                                   start_time: String,
                                   vip_id: String,
                                   vip_level: String
                                 ) {}

case class DataPcenterMemVipLevelSql(
                                      vip_id: String,
                                      vip_level: String,
                                      start_time: String,
                                      end_time: String,
                                      last_modify_time: String,
                                      max_free: String,
                                      min_free: String,
                                      next_level: String,
                                      operator: String,
                                      dn: String
                                    ) {}
