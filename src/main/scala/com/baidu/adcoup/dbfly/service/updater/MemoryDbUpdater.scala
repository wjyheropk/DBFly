package com.baidu.adcoup.dbfly.service.updater

import com.baidu.adcoup.dbfly.util.DatabaseSupport
import com.baidu.adcoup.dbfly.util.ETLContext._

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * 数据库数据增量更新服务，采用对比更新方式
 * 根据application.conf中的配置，将数据从crm_tmp库增量更新到crm库
 *
 * @author wangjiayin@baidu.com
 * @since 2015-07-10
 */
trait MemoryDbUpdater extends DatabaseSupport {

  // 源库
  val ds_src = crmTmpDs
  val rsExec_src = crmTmpRsExec
  val stExec_src = crmTmpStExec
  // 目的库
  val ds_des = crmDs
  val rsExec_des = crmRsExec
  // 默认划分份数，用于big_table增量更新
  lazy val DEFAULT_DIVIDE_NUM = 10

  def updateTables(productLine: String) = {
    val tableInfos = getTableInfos(s"$productLine.update.tables")
    updateTablesInternal(tableInfos)
  }

  /**
   * 新方法：更新表，自动判断是否有id字段
   * 说明：需要调用者控制表的更新顺序
   * @param tableInfos 三元组（表名，唯一列list，所有列list）
   */
  private def updateTablesInternal(tableInfos: mutable.Buffer[(String, List[String], List[String])]) = {

    // 对每个表进行处理，自动判断是否含有id
    tableInfos.foreach(info => {
      val colNameList = info._3
      val calNameListNoId = colNameList.filter(_ != "id")
      val hasId = colNameList.contains("id")
      val keyColNameList = info._2
      val tableName = info._1

      logger.info(s"processing table $tableName")
      // 用refColName来拆分数据，将数据分多次导入内存比较，避免内存溢出
      val srcTable = selectFromTable(rsExec_src, s"select * from $tableName", colNameList)
      val desTable = selectFromTable(rsExec_des, s"select * from $tableName", colNameList)
      val srcMap = table2Map(srcTable, keyColNameList)
      val desMap = table2Map(desTable, keyColNameList)

      val srcKeySet = srcMap.keySet
      val desKeySet = desMap.keySet
      val newSet = srcKeySet -- desKeySet
      val deleteSet = desKeySet -- srcKeySet
      val otherSet = srcKeySet & desKeySet
      val updateSet = for {
        key <- otherSet
        if ((srcMap(key) - "id" - "update_time") != (desMap(key) - "id" - "update_time"))
      } yield {
          key
        }

      // delete删除的记录
      if (deleteSet.nonEmpty) {
        logger.info(s"begin to delete from table $tableName")
        val deletePlaceHolder = keyColNameList.map(k => {
          "`" + k + "`" + "=?"
        }).mkString(" and ")
        deleteSet.map(k => {
          val valueMap = desMap(k)
          keyColNameList.map(k => {
            valueMap(k)
          })
        }).insert(ds_des, s"delete from $tableName where $deletePlaceHolder", deleteSet.size)
      }

      // update更新的记录
      // 更新时，无论表中有无id，都无需更新id列
      if (updateSet.nonEmpty) {
        logger.info(s"begin to update table $tableName")
        val updatePlaceHolder1 = calNameListNoId.map(k => {
          "`" + k + "`" + "=?"
        }).mkString(",")
        val updatePlaceHolder2 = keyColNameList.map(k => {
          "`" + k + "`" + "=?"
        }).mkString(" and ")
        updateSet.map(k => {
          val values = calNameListNoId.map {
            case col => srcMap(k)(col)
          }
          values ++ keyColNameList.map(col => srcMap(k)(col))
        }).insert(ds_des, s"update $tableName set $updatePlaceHolder1 where $updatePlaceHolder2", updateSet.size)
      }

      // insert新增的记录
      if (newSet.nonEmpty) {
        logger.info(s"begin to insert into table $tableName")
        val columns = colNameList.mkString("(`", "`,`", "`)")
        val placeHolders = colNameList.map(col => "?").mkString("(", ",", ")")
        var maxId = 1l
        if (hasId) {
          maxId = rsExec_des(rs => {
            rs.next
            rs.getLong("max_id")
          }, s"select max(id) as max_id from $tableName")
        }
        newSet.map(k => {
          val values = colNameList.map {
            case "id" =>
              maxId += 1
              maxId
            case col => srcMap(k)(col)
          }
          values
        }).insert(ds_des, s"insert into $tableName $columns values $placeHolders", newSet.size)
      }

      logger.info(s"processing table $tableName success")
    })
  }

  /**
   * 用于增量更新大数据量的表
   * key列：用于作为基准，来比较两条数据
   * 参考列：在key列中，类型为整数，用于将大数据量的表拆分成若干组小数据量
   * @param tableInfos 四元组：（表名，参考列，key列，所有列）
   */
  private def updateBigTable(tableInfos: mutable.Buffer[(String, String, List[String], List[String])]): Unit = {
    tableInfos.foreach(info => {
      val colNameList = info._4
      val calNameListNoId = colNameList.filter(_ != "id")
      val hasId = colNameList.contains("id")
      val keyColNameList = info._3
      val refColName = info._2
      val tableName = info._1
      val m = DEFAULT_DIVIDE_NUM

      logger.info(s"processing table $tableName")
      // 用refColName来拆分数据，将数据分多次导入内存比较，避免内存溢出
      (1 to m - 1).foreach(n => {
        val srcTable = selectFromTable(rsExec_src, s"select * from $tableName where $refColName % $m = $n", colNameList)
        val desTable = selectFromTable(rsExec_des, s"select * from $tableName where $refColName % $m = $n", colNameList)
        val srcMap = table2Map(srcTable, keyColNameList)
        val desMap = table2Map(desTable, keyColNameList)

        val srcKeySet = srcMap.keySet
        val desKeySet = desMap.keySet
        val newSet = srcKeySet -- desKeySet
        val deleteSet = desKeySet -- srcKeySet
        val otherSet = srcKeySet & desKeySet
        val updateSet = for {
          key <- otherSet
          if ((srcMap(key) - "id" - "update_time") != (desMap(key) - "id" - "update_time"))
        } yield key

        // delete删除的记录，并输出至文件
        if (deleteSet.nonEmpty) {
          logger.info(s"begin to delete from table $tableName")
          val deletePlaceHolder = keyColNameList.map(k => {
            "`" + k + "`" + "=?"
          }).mkString(" and ")
          deleteSet.map(k => {
            val valueMap = desMap(k)
            keyColNameList.map(k => {
              valueMap(k)
            })
          }).insert(ds_des, s"delete from $tableName where $deletePlaceHolder")
        }

        // update更新的记录，并输出至文件，有时间戳
        if (updateSet.nonEmpty) {
          logger.info(s"begin to update table $tableName")
          val updatePlaceHolder1 = calNameListNoId.map(k => {
            "`" + k + "`" + "=?"
          }).mkString(",")
          val updatePlaceHolder2 = keyColNameList.map(k => {
            "`" + k + "`" + "=?"
          }).mkString(" and ")
          updateSet.map(k => {
            val values = calNameListNoId.map {
              case col => srcMap(k)(col)
            }
            values ++ keyColNameList.map(col => srcMap(k)(col))
          }).insert(ds_des, s"update $tableName set $updatePlaceHolder1 where $updatePlaceHolder2")
        }

        // insert新增的记录，并输出至文件，有时间戳
        if (newSet.nonEmpty) {
          logger.info(s"begin to insert into table $tableName")
          val columns = colNameList.mkString("(`", "`,`", "`)")
          val placeHolders = colNameList.map(col => "?").mkString("(", ",", ")")
          var maxId = 1l
          if (hasId) {
            maxId = rsExec_des(rs => {
              rs.next
              rs.getLong("max_id")
            }, s"select max(id) as max_id from $tableName")
          }
          newSet.map(k => {
            val values = colNameList.map {
              case "id" =>
                maxId += 1
                maxId
              case col => srcMap(k)(col)
            }
            values
          }).insert(ds_des, s"insert into $tableName $columns values $placeHolders")
        }

      })

      logger.info(s"processing table $tableName success")
    })
  }


  /**
   * 通过路径字符串，获取相应的配置
   * @param configPath 配置路径
   * @return 三元组序列（表名，唯一列list，所有列list）
   */
  private def getTableInfos(configPath: String) = {
    crmConnExec(conn => {
      val md = conn.getMetaData
      val configs = config.getConfigList(configPath)
      configs.map(c => {
        val table = c.getString("name")
        (table, c.getStringList("key").toList, md.getColumns(null, null, table, null).map(rs => rs.getString(4)).toList)
      })
    })
  }

  /**
   * 通过路径字符串，获取相应的配置
   * @param configPath 配置路径
   * @return 四元组序列（表名，参考列，唯一列list，所有列list）
   */
  private def getBigTableInfos(configPath: String) = {
    crmConnExec(conn => {
      val md = conn.getMetaData
      val configs = config.getConfigList(configPath)
      configs.map(c => {
        val table = c.getString("name")
        (table, c.getString("ref"), c.getStringList("key").toList, md.getColumns(null, null, table, null).map(rs => rs.getString(4)).toList)
      })
    })
  }

  /**
   * 将数据转换成一个Map，key:唯一列值List，value:所有列的键值对Map
   * @param table 表中数据
   * @param keyColNameList key列list
   * @return
   */
  private def table2Map(table: List[Map[String, Object]], keyColNameList: List[String]) = {
    table.map(line => {
      (keyColNameList.map(k => line(k)), line)
    }).toMap
  }

  /**
   * 从数据库表中读数到内存
   * @param rsExec ResultSetExecutor
   * @param sql sql语句
   * @param colNameList 所需读取的列名list
   * @return
   */
  private def selectFromTable(rsExec: ResultSetExecutor, sql: String, colNameList: List[String]) = {
    rsExec(rs => {
      rs.map(r => {
        colNameList.map(colName => {
          colName -> r.getObject(colName)
        }).toMap
      }).toList
    }, sql)
  }

}