package com.fly.db.util

import java.sql.{Connection, ResultSet, SQLException, Statement}
import javax.sql.DataSource

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.pattern.gracefulStop
import akka.routing.{Broadcast, RoundRobinPool}
import com.typesafe.config.Config
import org.apache.commons.dbcp.BasicDataSource

import scala.collection.TraversableOnce
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * 数据库操作工具类，继承后使用
 *
 * @author wangjiayin
 * @since 2015-07-10
 */
trait DatabaseSupport extends LoggerSupport with ConfigSupport {

  // 默认批量提交大小
  val batchSize = 3000

  /**
   * 生成一个新的dataSource
   * @param url 连接url
   * @param username 用户名
   * @param password 密码
   * @param driverClassName 驱动，默认为Mysql
   * @return javax.sql.DataSource
   */
  def newDs(url: String, username: String, password: String, driverClassName: String = "com.mysql.jdbc.Driver") = {
    val ds = new BasicDataSource
    ds.setDriverClassName(driverClassName)
    ds.setUrl(url)
    ds.setUsername(username)
    ds.setPassword(password)
    ds
  }

  // tmp数据库（写数据库）
  val crmTmpDs = newDs(CRM_TMP_DB_URL, CRM_TMP_DB_USERNAME, CRM_TMP_DB_PASSWORD)
  val crmTmpStExec = new StatementExecutor(crmTmpDs)
  val crmTmpRsExec = new ResultSetExecutor(crmTmpDs)

  // 最终数据库（读数据库）
  val crmDs = newDs(CRM_DB_URL, CRM_DB_USERNAME, CRM_DB_PASSWORD)
  val crmStExec = new StatementExecutor(crmDs)
  val crmRsExec = new ResultSetExecutor(crmDs)
  val crmConnExec = new ConnectionExecutor(crmDs)

  // 源数据库
  val crmSrcDs = newDs(CRM_SRC_DB_URL, CRM_SRC_DB_USERNAME, CRM_SRC_DB_PASSWORD)
  val crmSrcStExec = new StatementExecutor(crmSrcDs)
  val crmSrcRsExec = new ResultSetExecutor(crmSrcDs)

  // 用于保存数据库连接
  val dsPool = scala.collection.mutable.Map[String, BasicDataSource]()
  dsPool += (CRM_TMP_DB_URL -> crmTmpDs)
  dsPool += (CRM_DB_URL -> crmDs)
  dsPool += (CRM_SRC_DB_URL -> crmSrcDs)

  // 通过配置获取数据库链接
  def getDataSource(config: Config) = {
    val host = config.getString("host")
    val port = config.getString("port")
    val username = config.getString("username")
    val password = config.getString("password")
    val dbName = config.getString("db_name")
    val url = s"jdbc:mysql://$host:$port/$dbName?useUnicode=true&characterEncoding=utf8&&autoReconnect=true&failOverReadOnly=false"
    if (dsPool.get(url) == None) {
      val ds = newDs(url, username, password)
      dsPool += (url -> ds)
      ds
    } else {
      dsPool.get(url).get
    }
  }

  /**
   * 所有Executor基类
   */
  abstract class Executor[T](ds: DataSource) {
    def apply[B](handle: T => B, params: Any*): B
  }

  class ConnectionExecutor(ds: DataSource) extends Executor[Connection](ds) {
    def apply[B](handle: Connection => B, params: Any*): B = {
      val c = ds.getConnection
      val r = handle(c)
      c.close()
      r
    }
  }

  class StatementExecutor(ds: DataSource) extends Executor[Statement](ds) {
    def apply[B](handle: Statement => B, params: Any*): B = {
      val c = ds.getConnection
      val s = c.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY)
      s.setFetchSize(batchSize)
      val r = handle(s)
      s.close()
      c.close()
      r
    }

    def getDataSource = ds
  }

  class ResultSetExecutor(ds: DataSource) extends Executor[ResultSet](ds) {
    def apply[B](handle: ResultSet => B, params: Any*): B = {
      val c = ds.getConnection
      val s = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      s.setFetchSize(Integer.MIN_VALUE)
      val rs = s.executeQuery(params(0).asInstanceOf[String])
      val r = try {
        handle(rs)
      } catch {
        case e: SQLException =>
          logger.error(e.getMessage, e)
          throw e
      }
      if (!rs.isClosed)
        rs.close()
      s.close()
      c.close()
      r
    }
  }

  /**
   * 数据库插入器
   */
  trait Inserter {
    def insert(values: Seq[Any])

    def close()
  }

  trait InserterSupport extends Inserter {
    val sql: String
    val ds: DataSource
    val batch: Int = batchSize
    private var count = 0

    lazy private val conn = {
      val c = ds.getConnection
      c.setAutoCommit(false)
      c
    }

    lazy private val ps = conn.prepareStatement(sql)

    def insert(values: Seq[Any]) = {
      values.zipWithIndex.foreach {
        case (value, i) =>
          ps.setObject(i + 1, value)
      }
      count += 1
      ps.addBatch()
      if (count % batch == 0)
        execute()
    }

    def close() = {
      execute()
      ps.close()
      conn.close()
    }

    private def execute() = {
      logger.info(s"batch commit count: $count")
      ps.executeBatch()
      conn.commit()
      ps.clearBatch()
    }
  }

  /**
   * akka多线程batch插入
   */
  case class AkkaInserter(sql: String, ds: DataSource, nr: Int = 0, batch: Int = batchSize) extends Inserter {
    inserter =>

    val system = ActorSystem("inserter")

    private val insertActor = system.actorOf(Props(new InsertActor).withRouter(RoundRobinPool(if (nr > 0) nr else 10))
      , "insertActor")

    class InsertActor extends Actor with InserterSupport {
      override val sql = inserter.sql
      override val ds = inserter.ds

      def receive = {
        case values: Seq[Any] => insert(values)
      }

      override def postStop() = {
        close()
      }
    }

    def insert(values: Seq[Any]) = {
      insertActor ! values
    }

    def close() = {
      try {
        val timeout = 1.hour
        Await.result(gracefulStop(insertActor, timeout, Broadcast(PoisonPill)), timeout)
        system.shutdown()
      } catch {
        case e: akka.pattern.AskTimeoutException =>
          logger.error("commit inserter timeout")
          throw e
      }
    }
  }

  /**
   * 使TraversableOnce[Seq[Any]]类型实例直接调用.insert方法实现插入数据库
   */
  implicit class SeqInserter[T](seq: TraversableOnce[Seq[Any]]) {
    def insert(ds: DataSource, sql: String, totalNum: Int = 0, nr: Int = 0) = {
      val nr = totalNum match {
        case _ if (totalNum / batchSize + 1) > 10 => 10
        case _ => totalNum / batchSize + 1
      }
      val inserter = AkkaInserter(sql, ds, nr)
      seq.foreach(v => inserter.insert(v))
      inserter.close()
    }
  }

  /**
   * 根据表名和列名，自动生成所需的insert语句
   * @param table 表名
   * @param columns 列名列表
   * @return sql语句
   */
  def autoGenInsertSql(table: String, columns: Seq[String]) = {
    val nameStr = columns.mkString("(", ",", ")")
    val valueStr = columns.map(_ => "?").mkString("(", ",", ")")
    s"insert into $table $nameStr values $valueStr"
  }

}
