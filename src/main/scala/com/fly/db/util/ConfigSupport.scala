package com.fly.db.util

import com.typesafe.config.{Config, ConfigFactory}

/**
 * 配置读取工具
 * 读取application.conf中的配置
 *
 * @author wangjiayin@baidu.com
 * @since 2015-07-10
 */
trait ConfigSupport {

  lazy val config = ConfigFactory.load()

  // crm 数据库配置
  lazy val CRM_DB_URL = config.getString("db.crm.url")
  lazy val CRM_DB_USERNAME = config.getString("db.crm.username")
  lazy val CRM_DB_PASSWORD = config.getString("db.crm.password")
  lazy val CRM_DB_NAME = config.getString("db.crm.db_name")

  // crm temp 数据库配置
  lazy val CRM_TMP_DB_URL = config.getString("db.crm_tmp.url")
  lazy val CRM_TMP_DB_USERNAME = config.getString("db.crm_tmp.username")
  lazy val CRM_TMP_DB_PASSWORD = config.getString("db.crm_tmp.password")
  lazy val CRM_TMP_DB_NAME = config.getString("db.crm_tmp.db_name")

  // crm src 数据库配置
  lazy val CRM_SRC_DB_URL = config.getString("db.crm_src.url")
  lazy val CRM_SRC_DB_USERNAME = config.getString("db.crm_src.username")
  lazy val CRM_SRC_DB_PASSWORD = config.getString("db.crm_src.password")
  lazy val CRM_SRC_DB_NAME = config.getString("db.crm_src.db_name")

  /**
   * 将配置路径字符串隐式转换为config对象
   * @param path 配置路径
   * @return config对象
   */
  implicit def str2Config(path: String): Config = config.getConfig(path)
}

