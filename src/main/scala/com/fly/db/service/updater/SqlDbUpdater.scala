package com.fly.db.service.updater

import com.fly.db.util.DatabaseSupport
import com.fly.db.util.ETLContext._
import com.fly.db.util.DatabaseSupport

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 数据库数据增量更新服务
  * 更新方式：使用SQL语言进行表对比更新
  * 根据application.conf中的配置，将数据从crm_tmp库增量更新到crm库
  *
  * 2016-11-18日修改：支持范围增量更新，而不是全表增量更新
  * 增加配置项：condition: "stat_day>='2016-07-01'"
  *
  * @author wangjiayin@baidu.com
  * @since 2016-01-10
  */
trait SqlDbUpdater extends DatabaseSupport {

  // 源库
  val ds_src = crmTmpDs
  val rsExec_src = crmTmpRsExec
  val stExec_src = crmTmpStExec
  // 目的库
  val ds_des = crmDs
  val rsExec_des = crmRsExec
  // 数据库名
  val srcDBName = ds_src.getConnection.getCatalog
  val desDBName = ds_des.getConnection.getCatalog

  /**
    * 此方法对外部提供
    */
  def updateTables(productLine: String) = {
    val tableInfos = getTableInfos(s"$productLine.update.tables")
    updateTablesInternal(tableInfos)
  }


  /**
    * 更新表，自动判断是否有id字段
    * 说明：需要调用者控制表的更新顺序
    *
    * @param tableInfos 三元组（表名，唯一列list，所有列list）
    */
  private def updateTablesInternal(tableInfos: mutable.Buffer[(String, List[String], List[String], String, Boolean)]) = {

    // 对每个表进行处理，自动判断是否含有id
    tableInfos.foreach(info => {

      val tableName = info._1
      val keyColNameList = info._2
      val colNameList = info._3
      val calNameListNoId = colNameList.filter(_ != "id")
      val valueColNameList = calNameListNoId.filter(!keyColNameList.contains(_))
      var condition = ""
      val needCheck = info._5

      // 2016-07-07日修改：更新之前，判断源表中，是否有keyCol重复的行
      if (needCheck) {
        logger.info(s"processing table $tableName, check repeated rows...")
        val keyColNameStr = keyColNameList.mkString(",")
        val checkSql = s"SELECT $keyColNameStr,COUNT(1) AS num FROM $tableName GROUP BY $keyColNameStr HAVING num>1"
        if (crmTmpRsExec(_.size, checkSql) > 0)
          throw new RuntimeException(s"Update $tableName failed, there're repeated rows")
      }

      // --------- delete ---------
      logger.info(s"processing table $tableName, begin to delete...")
      val joinSQL = keyColNameList.map(k => s"t1.`$k`=t2.`$k`").mkString(" AND ")
      if (info._4 != null) condition = s" AND t1.${info._4} "
      val deleteSQL =
        s"""DELETE t1 FROM $desDBName.`$tableName` t1
            LEFT JOIN $srcDBName.`$tableName` t2 ON $joinSQL
            WHERE t2.`${keyColNameList.head}` IS NULL $condition"""
      val deleteRowNum = crmStExec(_.executeUpdate(deleteSQL))
      logger.info(s"processing table $tableName, end to delete, $deleteRowNum rows deleted.")

      //  --------- update ---------
      logger.info(s"processing table $tableName, begin to update...")
      // 2016-01-27日修改，对于default null字段，不能单纯的用<>比较，因为NULL值是特殊值
      val updateCondition = valueColNameList.map(k => s"t1.`$k`<>t2.`$k` OR (t1.`$k` IS NULL AND t2.`$k` IS NOT NULL) OR (t1.`$k` IS NOT NULL AND t2.`$k` IS NULL)")
        .mkString("(", " OR ", ")")
      val updateSet = valueColNameList.map(k => s"t1.`$k`=t2.`$k`").mkString(",")
      if (info._4 != null) condition = s" AND t1.${info._4} "
      val updateSQL =
        s"""UPDATE $desDBName.`$tableName` t1
            INNER JOIN $srcDBName.`$tableName` t2 ON $joinSQL
            AND $updateCondition $condition
            SET $updateSet"""
      val updateRowNum = crmStExec(_.executeUpdate(updateSQL))
      logger.info(s"processing table $tableName, end to update, $updateRowNum rows updated.")

      //  --------- insert ---------
      logger.info(s"processing table $tableName, begin to insert...")
      val insertColNames = calNameListNoId.mkString("(", ",", ")")
      val selectColNames = calNameListNoId.map(k => s"t2.`$k`").mkString(",")
      if (info._4 != null) condition = s" AND t2.${info._4} "
      val insertSQL =
        s"""INSERT INTO $desDBName.`$tableName` $insertColNames
            SELECT $selectColNames
            FROM $srcDBName.`$tableName` t2 LEFT JOIN $desDBName.`$tableName` t1
            ON $joinSQL
            WHERE t1.`${keyColNameList.head}` IS NULL $condition"""
      val insertRowNum = crmStExec(_.executeUpdate(insertSQL))
      logger.info(s"processing table $tableName, end to insert, $insertRowNum rows inserted.")

      // 2016-07-07日修改：更新之后，校验两个表的行数是否一致，避免因为有重复行，而导致的原本、目的表数据行数不一致
      val srcTalbeRows = crmTmpRsExec(_.map(_.getLong("num")).toList.head, s"select count(1) as num from $tableName")
      val desTalbeRows = crmRsExec(_.map(_.getLong("num")).toList.head, s"select count(1) as num from $tableName")
      if (srcTalbeRows != desTalbeRows) throw new RuntimeException(s"Update $tableName failed, row nums not the same!")
    })
  }

  /**
    * 通过路径字符串，获取相应的配置
    *
    * @param configPath 配置路径
    * @return 五元组序列（表名，唯一列list，所有列list，更新范围条件，是否需要校验重复列）
    */
  private def getTableInfos(configPath: String) = {
    crmConnExec(conn => {
      val md = conn.getMetaData
      val configs = config.getConfigList(configPath)
      configs.map(c => {
        val table = c.getString("name")
        val condition = if (c.hasPath("condition")) c.getString("condition") else null
        val needCheck = if (c.hasPath("need_check")) c.getBoolean("need_check") else true
        (table,
          c.getStringList("key").toList,
          md.getColumns(null, null, table, null).map(rs => rs.getString(4)).toList,
          condition,
          needCheck)
      })
    })
  }


}

