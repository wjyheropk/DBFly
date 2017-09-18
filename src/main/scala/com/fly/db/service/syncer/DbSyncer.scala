package com.fly.db.service.syncer

import com.fly.db.util.{ConfigSupport, LoggerSupport}
import com.fly.db.util.{ConfigSupport, LoggerSupport}

import scala.collection.JavaConversions._
import scala.sys.process._

/**
 * 数据库数据同步服务
 * 根据application.conf中的配置，从源数据库中同步数据到目的数据库
 * 子类继承该类后直接调用synDB()
 * 也用作ETL前的准备工作，也可以用于线上数据备份
 *
 * @author wangjiayin
 * @since 2015-07-15
 */
trait DbSyncer extends ConfigSupport with LoggerSupport {

  /**
   * 同步数据库方法
   */
  def synDB() = {

    // 获取目标数据库配置
    val toHost = config.getString("syn.toDB.host")
    val toPort = config.getString("syn.toDB.port")
    val toUsername = config.getString("syn.toDB.username")
    val toPassword = config.getString("syn.toDB.password")
    val toDbName = config.getString("syn.toDB.db_name")
    val syncDbs = config.getConfigList("syn.fromDB").toList

    syncDbs.map(db => {
      // 获取源数据库配置
      val dbName = db.getString("db.db_name")
      val host = db.getString("db.host")
      val port = db.getString("db.port")
      val username = db.getString("db.username")
      val password = db.getString("db.password")
      val tables = db.getString("tables")

      // 导出数据
      logger.info( s"""mysqldump -u$username -p$password -h$host -P$port --skip-lock-table $dbName $tables > /tmp/$dbName.sql""")
      s"""mysqldump -u$username -p$password -h$host -P$port --skip-lock-table $dbName $tables """ #> new java.io.File( s"""/tmp/$dbName.sql""") !

      // 导入数据
      logger.info( s"""mysql -u$toUsername -p$toPassword -h$toHost -P$toPort $toDbName < /tmp/$dbName.sql""")
      s"""mysql -u$toUsername -p$toPassword -h$toHost -P$toPort $toDbName """ #< new java.io.File( s"""/tmp/$dbName.sql""") !

      // 为防止不同系统中的表叫同一个名字，支持表重命名
      if (db.hasPath("rename")) {
        val rename = db.getConfigList("rename").toList
        rename.map(r => {
          val from = r.getString("from")
          val to = r.getString("to")
          logger.info( s"""mysql -u$toUsername -p$toPassword -h$toHost -P$toPort -e 'ALTER TABLE $from RENAME $to' $toDbName""")
          val cmd = Seq("mysql", s"-u$toUsername", s"-p$toPassword", s"-h$toHost", s"-P$toPort",
            "-e", s"DROP TABLE IF EXISTS $to; ALTER TABLE $from RENAME $to", toDbName)
          cmd !!
        })
      }

    })
  }

}