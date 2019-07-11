package com.sun.bigdata.warehouse.common.util

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: data-warehouse-spark
  * Package: com.sun.bigdata.warehouse.common.util
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/10 13:02
  */
object ConfigUtils {
  val condRb =  ResourceBundle.getBundle("condition")
  def getValueConfig(key:String):String={
    val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
    val properties = new Properties()
    properties.load(stream)
    properties.getProperty(key)
  }

  def getValueCondition(key : String): String ={
    val str: String = condRb.getString("condition.params.json")
    val jSONObject: JSONObject = JSON.parseObject(str)
    jSONObject.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(ConfigUtils.getValueConfig("hive.database.test"))
  }
}

