package cn.wangyan.release.etl.dm

import cn.wangyan.release.constant.ReleaseConstant
import cn.wangyan.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object DMReleaseCustomer {
  private val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

  //处理一个时间分区的数据
  def handleReleaseJob(sparkSession: SparkSession, appName: String, bdp_day: String) = {
    var begin = System.currentTimeMillis()

    try {

      //导入隐式转换及内置函数
      import sparkSession.implicits._
      import org.apache.spark.sql.functions._

      //设置存储模式和缓存级别
      val saveMode = SaveMode.Overwrite
      val storageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

      val DWCustomerColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()
      val whereCondition = col(ReleaseConstant.DEF_PARTITION)===lit(bdp_day)
      val DWCustomerDf: DataFrame = SparkHelper.readTableDate(sparkSession,ReleaseConstant.DW_RELEASE_CUSTOMER,DWCustomerColumns)
        .where(whereCondition)
        //缓存数据
        .persist(storageLevel)

      DWCustomerDf.show()

      //=================渠道用户统计====================
      val customerSourceGroupColumns = Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}"
      )

      //DM层渠道用户统计字段
      val customerSourceColumns = DMReleaseColumnsHelper.selectDMReleaseCustomerSourcesColumns()

      val sourcesDf = DWCustomerDf.groupBy(customerSourceGroupColumns:_*)
        .agg(
          countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(ReleaseConstant.COL_RELEASE_USER_COUNT), //as "user_count"
          count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(ReleaseConstant.COL_RELEASE_TOTAL_COUNT)
        )
        .withColumn(ReleaseConstant.DEF_PARTITION,lit(bdp_day))
        .selectExpr(customerSourceColumns:_*)

      sourcesDf.show()

      //SparkHelper.writeTableData(sourcesDf,ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCES,saveMode)

      //=================目标客户多维统计====================
      //分组聚合列
      val customerCubeGroupColumns = Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
        $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
        $"${ReleaseConstant.COL_RELEASE_GENDER}",
        $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
      )

      val DMCustomerCubeColumns = DMReleaseColumnsHelper.selectDMReleaseCustomerCubeColumns()
      val customerCubeDf = DWCustomerDf.groupBy(customerCubeGroupColumns:_*)
        .agg(
          countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(ReleaseConstant.COL_RELEASE_USER_COUNT), //as "user_count"
          count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(ReleaseConstant.COL_RELEASE_TOTAL_COUNT)
        )
        .withColumn(ReleaseConstant.DEF_PARTITION,lit(bdp_day))
        .selectExpr(DMCustomerCubeColumns:_*)

      customerCubeDf.show()

      //      SparkHelper.writeTableData(customerCubeDf,ReleaseConstant.DM_RELEASE_CUSTOMER_CUBE,saveMode)


    }catch {
      case ex: Exception => {
        logger.error(ex.getMessage,ex)
      }
    }finally {
      println(s"任务处理时长：${appName},bdp_day=${bdp_day},${System.currentTimeMillis() - begin}")
    }
  }

  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String): Unit ={
    var spark:SparkSession =null
    try{
      // 配置Spark参数
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
      // 创建上下文
      spark = SparkHelper.createSparkSession(conf)
      // 解析参数
      val timeRange = SparkHelper.rangeDate(bdp_day_begin,bdp_day_end)
      // 循环参数
      for(bdp_day <- timeRange){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark,appName,bdp_date)
      }
    }catch {
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "dm_release_job"
    val bdp_day_begin = "20190924"
    val bdp_day_end = "20190924"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }




}
