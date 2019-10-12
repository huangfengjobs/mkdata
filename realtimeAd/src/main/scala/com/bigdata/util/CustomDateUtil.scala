package com.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

object CustomDateUtil {

  /**
   * 将指定的时间戳转换为时间字符串
   * @param ts
   * @param f
   * @return
   */
  def formatTime(ts:Long,f:String):String={
    formatDate(new Date(ts),f)
  }

  def formatDate(dt:Date,f:String):String={
    val sdf = new SimpleDateFormat(f)
    sdf.format(dt)
  }

  /**
   * 将字符串转换为时间戳
   * @param dateString
   * @param f
   * @return
   */
  def getTimestamp(dateString:String,f:String): Long ={
    val sdf = new SimpleDateFormat(f)
    sdf.parse(dateString).getTime
  }

}
