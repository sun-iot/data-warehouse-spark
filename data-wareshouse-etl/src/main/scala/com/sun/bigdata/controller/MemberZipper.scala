package com.sun.bigdata.controller

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: data-warehouse-spark
  * Package: com.sun.bigdata.controller
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/11 11:16
  */
object MemberZipper {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MemberZipper")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.sql("use bdl")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//    val time: String = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
    val time = "20190710"
    //查询当天增量数据
    val dayResult = spark.sql(s"select a.uid,sum(a.paymoney) as paymoney,max(b.vip_level) as vip_level," +
      s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,first(a.website) as website " +
      " from bdl.bdl_pcentermempaymoney a join " +
      s"bdl.bdl_vip_level b on a.vip_id=b.vip_id and a.website=b.website where a.time='$time' group by uid").as[MemberZipper]

    // 查询历史数据
    val historyZipper: Dataset[MemberZipper] = spark.sql("select * from idl.idl_member_zipper").as[MemberZipper]

    // 两份数据根据用户ID进行聚合，对end_time进行重新修改
    dayResult.union(historyZipper).groupByKey(item=>item.uid+"_"+item.website)
      .mapGroups{
        case (key , iters)=>{
          val keys: Array[String] = key.split("_")
          val uid = keys(0)
          val website = keys(1)
          // 对时间开始进行排序
          val zippers: List[MemberZipper] = iters.toList.sortBy(item=>item.start_time)
          // 如果 zippers存在历史数据，则要对end_time进行修改
          if (zippers.size >1 ){
            // 拿到历史数据的最后一条
            val old: MemberZipper = zippers(zippers.size - 2)
            // 获取当前时间的最后一条
            val current: MemberZipper = zippers(zippers.size - 1)
          }
          MemberZipperResult(zippers)
        }
          // 对对象进行重组，刷新拉链表
      }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("idl.idl_member_zipper")
  }
}

case class MemberZipper(
                         uid: String,
                         paymoney: String,
                         vip_level: String,
                         start_time: String,
                         var end_time: String,
                         website: String
                       )

case class MemberZipperResult(list: List[MemberZipper])
