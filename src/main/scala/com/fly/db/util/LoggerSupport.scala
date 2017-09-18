package com.fly.db.util

import org.slf4j.LoggerFactory

/**
 * 日志工具类
 *
 * @author wangjiayin
 * @since 2015-07-10
 */
trait LoggerSupport {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
}
