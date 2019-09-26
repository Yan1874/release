package cn.wangyan.release.etl.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {
  def selectDMReleaseCustomerCubeColumns() = {
    val columns = new ArrayBuffer[String]()

    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")

    columns
  }

  def selectDMReleaseCustomerSourcesColumns() = {
    val columns = new ArrayBuffer[String]()

    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")

    columns
  }


  //DW层目标客户主题字段
  def selectDWReleaseCustomerColumns() ={
    val columns = new ArrayBuffer[String]()

    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")

    columns
  }
}
