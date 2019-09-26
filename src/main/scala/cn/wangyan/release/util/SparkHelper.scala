package cn.wangyan.release.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object SparkHelper {
  private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  def rangeDate(bdp_day_begin: String, bdp_day_end: String): Seq[String] = {
    val bdp_days = new ArrayBuffer[String]()

    try {
      val begin: String = DateUtil.dateFormat4String(bdp_day_begin,"yyyyMMdd")
      val end: String = DateUtil.dateFormat4String(bdp_day_end,"yyyyMMdd")
      //开始时间与结束时间是或否相同
      if(begin.equals(end)) {
        bdp_days.+=(begin)
      }else {
        var cday = begin
        while(cday<end) {
          bdp_days.+=(cday)
          cday = DateUtil.dateFormat4StringDiff(cday,1,"yyyyMMdd")
        }
      }
    }catch {
      case ex: Exception => {
        println("参数不匹配")
        logger.error(ex.getMessage,ex)
      }
    }

    bdp_days
  }


  /**
    * 创建SparkSession
    *
    * @param conf
    * @return
    */
  def createSparkSession(conf: SparkConf): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //注册udf函数
    sparkSession.udf.register("getAgeRange",get_ageRange _)
    sparkSession
  }

  def get_ageRange(age: String) = {
    CommonUtil.getAgeRange(age)
  }

  /**
    * 将数据写入目标表
    *
    * @param tableDf
    * @param tableName
    * @param saveMode
    */
  def writeTableData(tableDf: DataFrame, tableName: String, saveMode: SaveMode) = {
    tableDf.write.mode(saveMode).insertInto(tableName)
  }

  /**
    * 读取表数据
    *
    * @param sparkSession
    * @param tableName
    * @param Columns
    * @return
    */
  def readTableDate(sparkSession: SparkSession, tableName: String, Columns: ArrayBuffer[String]) = {

    val tableDf: DataFrame = sparkSession.read.table(tableName)
      .selectExpr(Columns:_*)
    tableDf
  }



}
