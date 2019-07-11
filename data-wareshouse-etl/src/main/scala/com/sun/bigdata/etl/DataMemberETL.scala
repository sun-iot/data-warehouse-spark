package com.sun.bigdata.etl

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Copyright (c) 2018-2028 China All Rights Reserved
  *
  * Project: data-warehouse-spark
  * Package: com.sun.bigdata
  * Version: 1.0
  * 对数据进行清洗 ETL
  * -DHADOOP_USER_NAME=atguigu
  * bdl_base_ad
  * bdl_base_website
  * bdl_member
  * bdl_member_regtype
  * bdl_member_wxbound
  * bdl_pcentermempaymoney
  * bdl_vip_level
  * bdl_wxtype
  * Created by SunYang on 2019/7/10 10:26
  */
object DataMemberETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataETL")
    // 得到一个SparkSession对象 ， 并实现对Hive的支持
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import session.implicits._
    val path = "F:\\HadoopProject\\data-warehouse-spark\\data-wareshouse-etl\\src\\main\\resources\\Member.log"
    val memberSource: DataFrame = session.read.json(path)
    memberSource.show()
    val memberRDD: RDD[Member] = memberSource.as[Member].rdd
    session.sql("use bdl")
    session.sql("set hive.exec.dynamic.partition=true")
    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    memberRDD.mapPartitions {
      member => {
        member.filter(line => line != null).map {
          data => {
            val tuple: (String, String) = data.phone.splitAt(3)
            data.phone = tuple._1 + "*****" + tuple._2.splitAt(5)._2
            data.password = "******"
            val tupleName: (String, String) = data.fullname.splitAt(1)
            data.fullname = tupleName._1 + "XX"
            // 日期格式化操作
            //val time: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            val time: String = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
            MemberSql(data.uid, data.ad_id, data.birthday, data.email, data.fullname, data.iconurl, data.lastlogin, data.mailaddr, data.memberlevel, data.password, data.paymoney, data.phone, data.qq, data.register, data.regupdatetime, data.unitname, data.userip, data.zipcode, time, data.dn)
          }
        }
      }
    }.toDF().coalesce(2).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_member")
    session.stop()

  }
}

case class Member(
                   var ad_id: String,
                   var birthday: String,
                   var dn: String,
                   var email: String,
                   var iconurl: String,
                   var fullname: String,
                   var lastlogin: String,
                   var mailaddr: String,
                   var memberlevel: String,
                   var password: String,
                   var paymoney: String,
                   var phone: String,
                   var qq: String,
                   var register: String,
                   var regupdatetime: String,
                   var uid: String,
                   var unitname: String,
                   var userip: String,
                   var zipcode: String
                 ) {

}

case class MemberSql(
                      var uid: String,
                      var ad_id: String,
                      var birthday: String,
                      var email: String,
                      var fullname: String,
                      var iconurl: String,
                      var lastlogin: String,
                      var mailaddr: String,
                      var memberlevel: String,
                      var password: String,
                      var paymoney: String,
                      var phone: String,
                      var qq: String,
                      var register: String,
                      var regupdatetime: String,
                      var unitname: String,
                      var userip: String,
                      var zipcode: String,
                      var time: String,
                      var website: String
                    ) {

}
