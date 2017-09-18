package com.fly.db.service.importer

import com.fly.db.util.{DatabaseSupport, FileSupport}
import com.fly.db.util.{DatabaseSupport, FileSupport}
import com.typesafe.config.Config

/**
  * 文件数据导入DB工具类
  *
  * @author wangjiayin <wangjiayin>
  * @since 2015/8/5 11:21
  */
trait FileImporter extends FileSupport with DatabaseSupport {

  // 默认的列分隔符
  val defaultSeparator = "\\t"

  /**
    * 文件数据导入DB核心方法
    *
    * @param files application.conf中文件的配置
    */
  def importFiles(files: List[Config]) = {

    files.foreach(fileConfig => {

      val filePath = fileConfig.getString("path")
      val charset = fileConfig.getString("charset")
      val separator = if (fileConfig.hasPath("separator")) fileConfig.getString("separator") else defaultSeparator
      val tableName = fileConfig.getString("table")
      logger.info(s"loading file: $filePath to table: $tableName")

      val desColumns = crmTmpRsExec(rs => {
        val md = rs.getMetaData
        1.to(md.getColumnCount).map(i => md.getColumnLabel(i))
      }, s"select * from $tableName limit 1").toList.filterNot("id".equalsIgnoreCase)

      // 解析文件，生成目标数据
      val desData = processFile(s => {
        s.getLines().toList.map(_.split(separator, -1).toList.map {
          case "" => null
          case value => value
        })
      }, filePath, charset)

      // 将目标数据入库
      if (desData.nonEmpty) {
        val insertSql = autoGenInsertSql(tableName, desColumns)
        desData.insert(crmTmpDs, insertSql)
      }

    })


  }

  /**
    * 文件数据导入DB核心方法（适用于需要对文件内容进行清洗的场景）
    *
    * @param files     application.conf中文件的配置
    * @param parseFile 文件解析函数，自定义
    */
  def importFiles(files: List[Config], parseFile: List[String] => List[Map[String, Any]]) = {

    // 解析文件的函数必须有
    if (parseFile == null)
      throw new RuntimeException("parseFile function can not be null!")

    files.foreach(fileConfig => {

      val filePath = fileConfig.getString("path")
      val charset = fileConfig.getString("charset")
      val tableName = fileConfig.getString("table")
      logger.info(s"loading file: $filePath to table: $tableName")

      // 解析文件，生成目标数据
      val desData = processFile(s => {
        val lines = s.getLines().toList
        parseFile(lines)
      }, filePath, charset)

      // 将目标数据入库
      if (desData.nonEmpty) {
        val desColumns = desData.head.keySet.toSeq
        val insertSql = autoGenInsertSql(tableName, desColumns)
        desData.map(rowMap => {
          desColumns.map(rowMap(_))
        }).insert(crmTmpDs, insertSql)
      }

    })

  }


}

