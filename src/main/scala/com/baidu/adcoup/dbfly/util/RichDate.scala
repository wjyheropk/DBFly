package com.baidu.adcoup.dbfly.util

import java.util.Date

/**
 * java.util.Date的扩展，简化Date比较的写法
 *
 * @author wangjiayin <wangjiayin@baidu.com>
 * @since 2015/12/2 14:52
 */
class RichDate(d: Date) {

  def >(that: Date): Boolean = d.after(that)

  def >=(that: Date): Boolean = d.after(that) || d.equals(that)

  def <(that: Date): Boolean = d.before(that)

  def <=(that: Date): Boolean = d.before(that) || d.equals(that)

  def ==(that: Date): Boolean = d.equals(that)

}
