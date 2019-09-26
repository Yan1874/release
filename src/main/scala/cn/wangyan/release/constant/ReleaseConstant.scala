package cn.wangyan.release.constant

import org.apache.spark.storage.StorageLevel

object ReleaseConstant {
  val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
  val DEF_PARTITION = "bdp_day"
  val DEF_SOURCE_PARTITIOM = 4

  //维度列
  val COL_RELEASE_SESSION_STATUS = "release_status"
  val COL_RELEASE_SOURCES = "sources"
  val COL_RELEASE_CHANNELS = "channels"
  val COL_RELEASE_DEVICE_TYPE = "device_type"
  val COL_RELEASE_DEVICE_NUM = "device_num"
  val COL_RELEASE_USER_COUNT = "user_count"
  val COL_RELEASE_TOTAL_COUNT = "total_count"
  val COL_RELEASE_AGE_RANGE = "age_range"
  val COL_RELEASE_GENDER = "gender"
  val COL_RELEASE_AREA_CODE = "area_code"

  //ods
  val ODS_RELEASE_SESSION = "ods_newrelease.ods_release_session"

  //dw
  val DW_RELEASE_CUSTOMER = "dw_newrelease.dw_release_customer"

  //dm
  val DM_RELEASE_CUSTOMER_SOURCES = "dm_newrelease.dm_customer_sources"
  val DM_RELEASE_CUSTOMER_CUBE = "dm_newrelease.dm_customer_cube"


}
