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
  * Created by SunYang on 2019/7/10 16:57
  */
object DataBaseWebETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataETL")
    // 得到一个SparkSession对象 ， 并实现对Hive的支持
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    val path = "F:\\HadoopProject\\data-warehouse-spark\\data-wareshouse-etl\\src\\main\\resources\\baswewebsite.log"
    val memberSource: DataFrame = session.read.json(path)
    memberSource.show()
    val memberRDD: RDD[BaseWebsite] = memberSource.as[BaseWebsite].rdd
    session.sql("use bdl")
    session.sql("set hive.exec.dynamic.partition=true")
    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    memberRDD.mapPartitions {
      partition => {
        partition.map {
          data => {
            val time: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            BaseWebsiteSql(data.siteid, data.sitename, data.siteurl,data.delete , data.createtime,data.creator,data.dn )
          }
        }
      }
    }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_base_website")
  }
}

case class BaseWebsite(
                        var createtime: String,
                        var creator: String,
                        var delete: String,
                        var dn: String,
                        var siteid: String,
                        var sitename: String,
                        var siteurl: String
                      ) {

}

case class BaseWebsiteSql(
                           var siteid: String,
                           var sitename: String,
                           var siteurl: String,
                           var delete: String,
                           var createtime: String,
                           var creator: String,
                           var dn: String
                         ) {

}
