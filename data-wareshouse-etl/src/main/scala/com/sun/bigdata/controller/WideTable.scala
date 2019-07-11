package com.sun.bigdata.controller

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: data-warehouse-spark
  * Package: com.sun.bigdata.controller
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/10 19:09
  */
object WideTable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WideTable")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.sql("use bdl")
    val wideTableDF: DataFrame = spark.sql("")
    wideTableDF.as[WideTableInit].rdd.toDF().write.mode(SaveMode.Overwrite).insertInto("idl.idl_member")
  }
}


case class WideTableInit(
                          uid: String,
                          ad_id: String,
                          email: String,
                          fullname: String,
                          icounurl: String,
                          lastlogin: String,
                          mailaddr: String,
                          memberlevel: String,
                          password: String,
                          paymoney: String,
                          phone: String,
                          qq: String,
                          register: String,
                          regupdatetime: String,
                          unitname: String,
                          userip: String,
                          zipcode: String,
                          time: String,
                          appkey: String,
                          appregurl: String,
                          bdp_uuid: String,
                          reg_createtime: String,
                          isranreg: String,
                          regsource: String,
                          regsourcename: String,
                          adname: String,
                          siteid: String,
                          sitename: String,
                          siteurl: String,
                          site_delete: String,
                          site_createtime: String,
                          site_creator: String,
                          vip_id: String,
                          vip_level: String,
                          vip_start_time: String,
                          vip_end_time: String,
                          vip_last_modify_time: String,
                          vp_max_free: String,
                          vip_min_free: String,
                          vip_next_level: String,
                          vip_operator: String,
                          website: String
                        ) {}