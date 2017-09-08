package com.baidu.adcoup.dbfly.service.importer

import java.sql.ResultSet

import com.baidu.adcoup.dbfly.util.DatabaseSupport
import com.baidu.adcoup.dbfly.util.ETLContext._
import com.typesafe.config.Config

import scala.collection.immutable.Map

/**
  * 数据库数据清洗导入服务
  * 根据application.conf中的配置，从源数据库中select数据insert到目的数据库
  * 数据清洗导入过程可以有业务逻辑处理
  *
  * @author wangjiayin@baidu.com
  * @since 2015-07-18
  */
trait DbImporter extends DatabaseSupport {

  /**
    * 清空crm_tmp库下面所有的表
    */
  def cleanAllTables() = {
    // 不需要每次清空的表
    val notTruncateTables = config.getStringList("load_init.not_truncate_table")
    logger.info(s"Begin to clean talbes, not truncate tables: $notTruncateTables")
    // crm_tmp的数据库名，默认为crm_tmp
    val dbName = CRM_TMP_DB_NAME
    crmTmpRsExec(rs => {
      // 获取数据库下面所有的表并清空
      rs.map(_.getString("TABLE_NAME")).filterNot(notTruncateTables.contains(_)).foreach(table => {
        logger.info(s"TRUNCATE table: $table")
        crmTmpStExec(s => s.executeUpdate(s"TRUNCATE table $table"))
      })
    }, s"SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='$dbName' AND table_type='base table'")
  }

  /**
    * 导入数据至crm_tmp的主函数
    * 2016-01-04修改：支持分表数据源
    *
    * @param tables 需要处理的table配置列表
    * @param handle 业务逻辑转换函数
    * @return
    */
  def loadTables(tables: List[_ <: Config],
                 handle: List[Map[String, Any]] => Iterable[Map[String, Any]] = null) = {

    tables.foreach(tableConfig => {
      // 源数据库
      val stExec = new StatementExecutor(autoGenSrcDB(tableConfig))
      // 目的数据库
      val desDb = autoGenDesDB(tableConfig)
      val desTableName = tableConfig.getString("table")
      val description = tableConfig.getString("description")
      logger.info(s"loading table to crm_tmp: $desTableName, description: $description")

      stExec(st => {

        // 考虑是否有分表的配置
        val sqlList = if (tableConfig.hasPath("src_table_divide_info")) {
          val divideConfig = tableConfig.getConfig("src_table_divide_info")
          val tableBaseName = divideConfig.getString("table_base_name")
          val divideNum = divideConfig.getInt("divide_num")
          val selectSql = divideConfig.getString("select")
          (0 to divideNum - 1).map(n => selectSql.replaceFirst("\\$table_name", tableBaseName + "_" + n))
        } else {
          List(tableConfig.getString("select"))
        }

        sqlList.foreach(selectSql => {
          // 从源中读取数据
          val rs = st.executeQuery(selectSql)
          // 源数据中的列
          val columns = {
            val md = rs.getMetaData
            1.to(md.getColumnCount).map(i => md.getColumnLabel(i))
          }.toList
          // 源数据经过处理后（可选）,导入到temp数据库
          handle match {
            // 简单插入，无业务逻辑
            case null =>
              // 获取结果集总数
              rs.last()
              val srcTotalNum = rs.getRow
              rs.beforeFirst()
              val insertSql = autoGenInsertSql(desTableName, columns)
              rs.map(r => columns.map(r.getObject)).insert(desDb, insertSql, srcTotalNum)
            // 有业务逻辑干预
            case _ =>
              // 取出后的源数据，List[Map[String, Any]
              val srcData = rs.map(r => columns.map(name => name -> r.getObject(name)).toMap).toList
              val desData = handle(srcData)
              if (desData.nonEmpty) {
                val srcTotalNum = desData.size
                val desColumns = desData.head.keySet.toList
                val insertSql = autoGenInsertSql(desTableName, desColumns)
                desData.map(rowMap => {
                  desColumns.map(rowMap(_))
                }).insert(desDb, insertSql, srcTotalNum)
              }
          }
          rs.close()
        })

      })
    })

  }

  /**
    * 使用update语句，导入数据至crm_tmp
    * 2016-01-26修改：数据过大时，会内存溢出，加入数据分片清洗，使用divide配置项
    *
    * @param tables 需要处理的table配置列表
    * @param handle 业务逻辑转换函数, 不能为Null
    * @return
    */
  def loadTablesWithUpdateSql(tables: List[Config], handle: List[Map[String, Any]] => Iterable[List[Any]]) = {

    if (handle == null) throw new RuntimeException("业务处理函数不能为空！")
    tables.foreach(tableConfig => {
      // 源数据库
      val stExec = new StatementExecutor(autoGenSrcDB(tableConfig))
      // 目的数据库
      val desDb = autoGenDesDB(tableConfig)
      // 要插入的表名
      val desTableName = tableConfig.getString("table")
      val description = tableConfig.getString("description")
      logger.info(s"loading table to crm_tmp with update sql: $desTableName, description: $description")
      stExec(st => {
        // 从源中读取数据
        val selectSql = tableConfig.getString("select")
        val updateSql = tableConfig.getString("update")
        // 看是否配置了divide参数
        val (divideBy, num) = if (tableConfig.hasPath("divide")) {
          val divideConfig = tableConfig.getConfig("divide")
          (divideConfig.getString("column"), divideConfig.getInt("num"))
        } else (null, 1)
        // 如果配置了divide参数，那么分片导入，否则的话，只循环一次
        (0 to num - 1).foreach(n => {
          val appendSql = divideBy match {
            case null => selectSql
            case _ =>
              logger.info(s"loading table to crm_tmp with udpate sql: $desTableName, divided by $divideBy, n=$n")
              s"$selectSql AND MOD($divideBy,$num)=$n"
          }
          val rs = st.executeQuery(appendSql)
          // 源数据中的列
          val columns = {
            val md = rs.getMetaData
            1.to(md.getColumnCount).map(i => md.getColumnLabel(i))
          }
          // 源数据经过处理后（可选）,导入到temp数据库，取出后的源数据，List[Map[String, Any]
          val srcData = rs.map(r => columns.map(name => name -> r.getObject(name)).toMap).toList
          val desData = handle(srcData)
          if (desData.nonEmpty) desData.insert(desDb, updateSql, desData.size)
          rs.close()
        })

      })
    })

  }

  /**
    * 读取application.conf配置，查询数据库并返回结果
    *
    * @param tableConfig 配置项
    * @param handle      对结果resultSet的处理函数
    * @param args        select sql中的参数
    * @tparam B 对结果处理的返回类型
    * @return
    */
  def simpleQueryData[B](tableConfig: Config, handle: ResultSet => B, args: List[String] = Nil) = {
    val desc = tableConfig.getString("description")
    logger.debug(s"Executing simple data query: $desc, args=$args")
    // 源数据库
    val stExec = new StatementExecutor(autoGenSrcDB(tableConfig))
    stExec(st => {
      // 从源中读取数据
      val selectSql = tableConfig.getString("select")
      val replacedSql = args.foldLeft(selectSql)(_.replaceFirst("\\?", _))
      val rs = st.executeQuery(replacedSql)
      val result = handle(rs)
      rs.close()
      result
    })
  }

  /**
    * 读取application.conf配置，查询数据库并返回结果
    *
    * @param tableConfig 配置项
    * @param handle      对结果resultSet的处理函数
    * @param args        select sql中的参数
    * @tparam B 对结果处理的返回类型
    * @return
    */
  def simpleQueryData1[B](tableConfig: Config, handle: ResultSet => B, args: Map[String, String] = Map.empty) = {
    val desc = tableConfig.getString("description")
    logger.debug(s"Executing simple data query: $desc, args=$args")
    // 源数据库
    val stExec = new StatementExecutor(autoGenSrcDB(tableConfig))
    stExec(st => {
      // 从源中读取数据
      val selectSql = tableConfig.getString("select")
      val replacedSql = args.foldLeft(selectSql)((result, kv) => result.replaceAll(kv._1, kv._2))
      logger.debug(s"Executing simple data query, sql=$replacedSql")
      val rs = st.executeQuery(replacedSql)
      val result = handle(rs)
      rs.close()
      result
    })
  }

  /**
    * 读取application.conf中的配置，并将数据插入库
    *
    * @param tableConfig 配置项
    * @param data        所需插入的数据
    */
  def simpleInsertData(tableConfig: Config, data: Iterable[Map[String, Any]]): Unit = {
    if (data.isEmpty) return
    val desc = tableConfig.getString("description")
    logger.info(s"Executing simple data insert: $desc")
    val tableName = tableConfig.getString("table")
    val dataSource = autoGenSrcDB(tableConfig)
    val srcTotalNum = data.size
    val desColumns = data.head.keySet.toList
    val insertSql = autoGenInsertSql(tableName, desColumns)
    data.map(rowMap => {
      desColumns.map(rowMap(_))
    }).insert(dataSource, insertSql, srcTotalNum)
  }

  /**
    * 根据配置生成源数据库，导入数据时，源数据库支持配置，默认的源数据库为crm_src
    *
    * @param tableConfig 插入表配置
    * @return
    */
  def autoGenSrcDB(tableConfig: Config) = {
    tableConfig.hasPath("src_db") match {
      // 如果有配置源数据库，则取配置
      case true =>
        val dbConfig = tableConfig.getConfig("src_db")
        getDataSource(dbConfig)
      // 默认的源数据库为crm_src数据库
      case _ => crmSrcDs
    }
  }

  /**
    * 根据配置生成目标数据库，导入数据时，目标数据库支持配置，默认导入到crm_tmp里
    *
    * @param tableConfig 插入表配置
    * @return
    */
  def autoGenDesDB(tableConfig: Config) = {
    tableConfig.hasPath("des_db") match {
      // 如果有配置源数据库，则取配置
      case true =>
        val dbConfig = tableConfig.getConfig("des_db")
        getDataSource(dbConfig)
      // 默认的源数据库为crm_tmp数据库
      case _ => crmTmpDs
    }
  }

}
