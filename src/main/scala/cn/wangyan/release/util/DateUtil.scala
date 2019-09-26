package cn.wangyan.release.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DateUtil {
  def dateFormat4String(date: String, format: String="yyyyMMdd"): String = {
    if(null == date) {
      return null
    }
    //如果时间不符合就会报错，符合就原样返回
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    val localDate: LocalDate = LocalDate.parse(date,formatter)
    val strDate: String = localDate.format(formatter)
    strDate
  }

  def dateFormat4StringDiff(date:String,diff:Long,format:String = "yyyyMMdd"): String={
    if(null == date) {
      return null
    }
    //如果时间不符合就会报错，符合就原样返回
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    val localDate: LocalDate = LocalDate.parse(date,formatter)
    val newDate: LocalDate = localDate.plusDays(diff)
    newDate.format(formatter)
  }

}
