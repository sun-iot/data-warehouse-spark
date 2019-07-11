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
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    var sql ="select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
      "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
      "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
      "first(time),first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain)," +
      "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
      "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
      "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
      "first(vip_operator),website from" +
      "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
      "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
      "a.zipcode,a.time,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource," +
      "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
      "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
      "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
      "f.next_level as vip_next_level,f.operator as vip_operator,a.website " +
      "from bdl.bdl_member a left join bdl.bdl_member_regtype b on a.uid=b.uid " +
      "and a.website=b.website left join bdl.bdl_base_ad c on a.ad_id=c.adid and a.website=c.website left join " +
      " bdl.bdl_base_website d on b.websiteid=d.siteid and b.website=d.website left join bdl.bdl_pcentermempaymoney e" +
      " on a.uid=e.uid and a.website=e.website left join bdl.bdl_vip_level f on e.vip_id=f.vip_id and e.website=f.website)r " +
      "group by uid,website"
    spark.sql(sql).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("idl.idl_member")

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
                          domain:String,
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