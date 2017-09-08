package com.baidu.adcoup.dbfly.util

import java.io.{File, PrintWriter}

import scala.io.Source
import scala.util.matching.Regex

/**
 * 文件操作工具类
 *
 * @author wangjiayin <wangjiayin@baidu.com>
 * @since 2015/12/2 15:04
 */
trait FileSupport extends ConfigSupport {

  // 生成csv文件时的字符串处理方法
  def escapeCsvValue(value: Any): String = {
    value match {
      case s: String => "\"" + s.replaceAll("\"", "\\\\\"") + "\""
      case _ => if (value == null) "" else value.toString
    }
  }

  // 生成mysql时的字符串处理方法
  def escapeMysqlValue(value: Any): String = {
    value match {
      case s: String => s.replaceAll("\"", "\\\"")
      case _ => if (value == null) "" else value.toString
    }
  }

  def escapeQuotationMark(value: Any): String = {
    value match {
      case s: String => s.replaceAll("\"", "\\\\\"")
      case _ => if (value == null) "" else value.toString
    }
  }

  // 在temp目录保存tmp文件，文件名带随机数
  def saveTempFile[T](f: PrintWriter => T): (File, T) = {
    val tempFile = File.createTempFile("brand", ".csv")
    val out = new PrintWriter(tempFile, "UTF-8")
    val r = f(out)
    out.close()
    (tempFile, r)
  }

  // 在当前工程路径下保存csv文件，指定文件名
  def saveFileInProjectPath[T](f: PrintWriter => T, name: String) = {
    val tempFile = new File(s"$name.csv")
    val out = new PrintWriter(tempFile, "UTF-8")
    val r = f(out)
    out.close()
    (tempFile, r)
  }

  // 在当前工程路径下保存csv文件，指定文件名
  def saveFileInClassPath[T](f: PrintWriter => T, name: String) = {
    val classPath = Thread.currentThread().getContextClassLoader.getResource("").getPath
    val tempFile = new File(classPath + name)
    val out = new PrintWriter(tempFile, "UTF-8")
    val r = f(out)
    out.close()
    (tempFile, r)
  }

  // 处理在 classpath 中的文件，主要用于导入
  def processFileInClassPath[T](f: Source => T, path: String, encoding: String = "GBK"): T = {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream(path)
    val s = Source.fromInputStream(is, encoding)
    val r = f(s)
    s.close()
    is.close()
    r
  }

  // 处理在 filesystem 中的文件，主要用于导入
  def processFile[T](f: Source => T, path: String, encoding: String = "GBK"): T = {
    val s = Source.fromFile(new File(path), encoding)
    val r = f(s)
    s.close()
    r
  }

  // 将csv保存在指定目录下
  def saveFileInAssignPath[T](f: PrintWriter => T, name: String, path: String) = {
    val tempFile = new File(s"$path" + File.separator + s"$name.csv")
    val out = new PrintWriter(tempFile, "UTF-8")
    val r = f(out)
    out.close()
    (tempFile, r)
  }

}

//object test extends FileSupport with DatabaseSupport with App {
//
//  val out = new PrintWriter("/Users/wangjiayin/Downloads/SME1.txt", "UTF-8")
//  val r = ".*tam-ogel[^\"]*\\.gif.*".r
//  Source.fromFile("/Users/wangjiayin/Downloads/SME.txt").getLines().filter(line => {
//    val material = line.split("\t")(6)
//    r.pattern.matcher(material).matches()
//  }).toList.foreach(out.println)
//  out.close()
//
//}