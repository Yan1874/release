package cn.wangyan.release.etl.dw

import cn.wangyan.release.constant.ReleaseConstant
import cn.wangyan.release.enums.ReleaseStatusEnum
import cn.wangyan.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * 目标客户主题
  */
object DWReleaseCustomer {
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  def handleReleaseJob(sparkSession: SparkSession, appName: String, bdp_day: String) ={
    val begin: Long = System.currentTimeMillis()

    try {

      //导入隐式转换
      import sparkSession.implicits._
      import org.apache.spark.sql.functions._


      //设置缓存级别
      val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
      //设置写入模式 overwrite
      val saveMode = SaveMode.Overwrite
      //获取解析后的每个字段
      val customerColumns: ArrayBuffer[String] = DWReleaseColumnsHelper.SelectDWReleaseCustomerColumns()
      //设置where过滤条件 status=01 bdp_day=
      val CustomerReleaseCondition = (col(ReleaseConstant.DEF_PARTITION) === lit(bdp_day)
        and col(ReleaseConstant.COL_RELEASE_SESSION_STATUS)===lit(ReleaseStatusEnum.CUSTOMER.getCode))
      //从ODS层获取DW层目标客户主题
      val tableDf: DataFrame = SparkHelper.readTableDate(sparkSession,ReleaseConstant.ODS_RELEASE_SESSION,customerColumns)
        .where(CustomerReleaseCondition)
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITIOM)

      //将数据写入表中
      //SparkHelper.writeTableData(tableDf,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)

      tableDf.show(10,false)

    }catch {
      case ex: Exception => {
        logger.error(ex.getMessage,ex)
      }
    }finally {
      println(s"任务处理时长：${appName},bdp_day=${bdp_day},${System.currentTimeMillis() - begin}")
    }

  }


  def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
    var sparkSession: SparkSession = null

    try {

      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")

      sparkSession = SparkHelper.createSparkSession(conf)
      val rangeDate: Seq[String] = SparkHelper.rangeDate(bdp_day_begin,bdp_day_end)

      for(bdp_day <- rangeDate) {
        handleReleaseJob(sparkSession,appName,bdp_day)
      }
    }catch {
      case ex: Exception => {
        logger.error(ex.getMessage,ex)
      }

    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "dw_release_job"
    val bdp_day_begin = "20190924"
    val bdp_day_end = "20190924"

  handleJobs(appName,bdp_day_begin,bdp_day_end)
  }


}
