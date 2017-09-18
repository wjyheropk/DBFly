package com.fly.db.util

import java.sql.ResultSet
import java.util.Date

/**
  * 隐式转换上下文，需要使用时导入
  *
  * @author wangjiayin <wangjiayin@baidu.com>
  * @since 2015/12/2 15:04
  */
object ETLContext {

  implicit def date2RichDate(d: Date): RichDate = new RichDate(d)

  implicit def rs2Iter(rs: ResultSet) = new Iterator[ResultSet] {
    def hasNext = {
      val b = rs.next()
      if (!b) rs.close()
      b
    }

    def next = rs
  }

}
