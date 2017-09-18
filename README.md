## What is DbFly?  ##

DbFly是一款用scala语言编写的、轻量级的、可扩展的mysql数据收集系统，它主要由数据同步子系统、数据清洗录入子系统、数据增量更新子系统三部分组成。

DBFly框架的目的是方便的将源数据，经过各种处理，变成你想要的数据结构，并存储到你自己的数据库中，以便使用。DbFly适用于需要从其他数据源定时采集分析录入数据，并需要进行增量更新的系统。这个过程是基于配置的，大部分情况下，你只需要在配置文件中增加一些配置，就可以完成数据采集录入和更新的工作，在一些特殊的复杂场景下，您也仅需实现一少部分scala代码，就可以完成该流程。

**关键词**：scala、配置、mysql、数据收集

![](https://raw.githubusercontent.com/wjyheropk/ImageStore/master/DBFly.jpg)

## Features: ##

1、使用scala语言开发，scala语言优雅，表达力强，善于数据计算和处理。
2、三个子系统各司其职，协调配合，低耦合、高内聚。
3、全面基于配置，免去了大量代码开发的工作，也不用为了充斥在代码各个角落的sql语句而烦恼

## Getting Started: ##

### 1、创建数据库
DbFly需要使用mysql，因为每个子系统工作时都需要独立的数据库，您需要为每个子系统创建一个数据库，假设数据同步子系统的数据库是syn-db，数据清洗子系统的数据库是load-db，增量更新子系统的数据库是update-db。为了保证整个数据同步、清洗、增量更新的流程稳定高效和便捷，多花一点存储空间是可以被接受的。

### 2、application.conf
application.conf是DbFly中最重要的配置文件。

- 配置数据源：

		db {
		  syn_db {
		  url: "jdbc:mysql://×××.com:3306/syn_db?rewriteBatchedStatements=true"
		  host: "×××.com"
		  port: "..."
		  username: "..."
		  password: "..."
		  db_name: "syn_db"
		  }
		  // other data source...
		}


	像上面这种方式定义数据源，数据源可以定义很多个，因为你可能需要从多个数据源中收集数据。当然，你也可以在配置文件中定义和使用变量，会简化和方便配置管理，像这样：

		jdbc_username: "root"
		jdbc_password: "root"
		// other params...

		db {
		  syn_db {
		    url: "jdbc:mysql://×××.com:3306/syn_db?rewriteBatchedStatements=true"
		    host: "×××.com"
		    port: "..."
		    username: ${jdbc_username}
		    password: ${jdbc_password}
		    db_name: "syn_db"
		  }
		  // other data source...
		}

### 3、使用数据同步服务

数据同步服务由数据同步子系统提供支持，您只需要简单的配置，即可将其他业务系统的数据同步到您的系统。

- application.conf配置：

		syn {
		  toDB: ${db.syn_db}
		  fromDB: [
		    {
		      db: ${db.src_db_1}
		      tables: """table1 table2"""
		    }
		    {
		      db: ${db.src_db_2}
		      tables: "table3 table4..."
		      rename: [
		        {from: "table3", to: "table3_new"}
		        {from: "table4", to: "table4_new"}
		      ]
		    }
		    // other source db...
		  ]
		}

	如上所示，syn.toDB用于配置同步的目的数据库，其中${db.syn_db}为前面我们配置好的数据源，syn.fromDB用于配置同步的源数据库，syn.fromDB.tables配置所需要同步的表，syn.fromDB.rename支持同步到目的数据库后表的重命名。

- DbSyncer：

	com.fly.db.service.syncer.DbSyncer提供了数据同步的核心服务，这是一个trait，想要使用它，你可以创建一个类继承DbSyncer，再调用它的synDB()方法，例如：

		/**
		 * 同步数据库任务
		 */
		object SyncDBTask extends DbSyncer with App {
		  synDB()
		}

	执行SyncDBTask，就可以根据application.conf中的配置，将数据从源数据库同步到目的数据库了。**注意：在这个过程中，数据是保持不变的，这一步骤仅仅是将数据原封不动的拷贝过来而已。**

- 为什么需要数据同步？：

	DBFly框架的目的是方便的将源数据，经过各种处理，变成你想要的数据结构，并存储到你自己的数据库中，以便使用，为什么要先同步过来再处理呢？原因是这样：在有些情况下，不同源的数据之间，可能存在关联，试想存储在不同数据库中的两个表，如何join操作？这时候就需要将它们原封不动的拷贝过来，存在同一个数据库中，从而方便做关联。

	还是那句话，为了提高系统的便利性、稳定性和可扩展性，为了提高开发效率，减少维护成本，我们任务多花费一点存储空间，是可以被接受的。

	当然，有些不需要关联的数据，就不必先同步过来再清洗处理，DBFly支持数据从源端直接进入清洗录入子系统，见下节。

### 4、使用数据清洗录入服务

数据的清洗、录入是整个过程中最重要的一步。因为现实情况中，源数据的格式或结构，几乎都是不符合我们的要求的。

- DbImporter
	com.fly.db.service.importer.DbImporter是一个trait，它是数据清洗子系统的核心，其中最重要的一个方法是：

		  /**
		   * 清洗并导入数据至load_db的主函数
		   * @param tables 需要处理的table配置列表
		   * @param handle 业务逻辑转换函数
		   * @return 无
		   */
		  def loadTables(tables: List[_ <: Config],
		                 handle: List[Map[String, Any]] => Iterable[Map[String, Any]] = null)

	函数的第一个参数是清洗配置项，第二个参数是业务处理函数（数据清洗函数），第二个参数默认为null，也就是说您可以不用传此参数。一般情况下，我们可以通过配置sql语句完成数据结构的转换，DbImporter会根据您的配置，将select出来的数据之间插入到目的表。但在有些情况下，例如源数据某个字段是Json格式，需要解析后再存储，那么这时候sql语句就无法完成处理，您就需要通过代码实现这一部分逻辑，并作为loadTables的第二个参数，传递进去。

- 简单场景（使用sql进行数据结构转换）

	即单纯使用sql语句就可以完成数据的处理和数据结构的转换，application.conf的配置如下：

		// 数据的清洗入库配置
		load {
		  // 简单数据清洗配置，不含业务逻辑处理，select之后的数据直接insert到目的表
		  // select之后的字段需要与目的表字段一一对应
		  simple: [
		    {
		      table: "day_stat"
		      description: "导入每日消费统计"
		      src_db: ${db.src_db_1}  // 可选
		      select: """SELECT stat_day AS date, SUM(cost) AS total_cost FROM day_stat GROUP BY stat_day"""
		    }
			// other simple load...
		  ]
		}

	然后您可以这样使用它：

		trait SimpleDbImporter extends DbImporter {

		   /**
		    * 从源数据中select之后，直接插入temp数据库
		    * 不需要经过业务逻辑处理
		    */
		   def loadSimpleData() = {
		     val tables = config.getConfigList("load.simple")
		     loadTables(tables.toList)
		   }

		 }

- 复杂场景（需要代码实现数据处理）

	这时候您可以这样使用：

		private def loadCustomisedData() = {

		loadTables(List("load.ka.contract_line"), srcData => {

			// 这里实现您的数据处理逻辑

		  })
		})

		}

- 源数据存储在文件中？

	DBFly也支持从源文件中读取数据并清洗入库，application.conf中配置原文件：

		load {
		  files: [
		    {
		      path: "D:/Code/tmp/tmp.txt"
		      charset: "UTF-8"
		      table: "..."
		    }
			// other source files...
		  ]
		}

	接下来您需要做的就是继承com.fly.db.service.importer.FileImporter并使用其中的importFiles方法：

		/**
		* 文件数据导入DB核心方法
		* @param files application.conf中文件的配置
		* @param parseFile 文件解析函数，自定义
		*/
		def importFiles(files: List[Config], parseFile: List[String] => List[Map[String, Any]])

	可以看到，它的实现与DbImporter的实现大相径庭，如果您需要业务逻辑处理，那么请传递第二个参数吧！

### 5、使用数据增量更新服务

有些时候，我们希望我们从其他系统抓取过来的数是经常更新的，那么我们可以将同步、清洗两个步骤做成定时任务，定时执行。

同步和清洗的是全量数据，但我们需要增量的更新到我们的系统中，因为这部分数据可能正在被使用，为了不影响现有数据的使用，我们不可能先把数据全量删除再全量插入，这种更新方法不是增量更新。

- application.conf的配置

		// 增量更新配置
		update {
		  tables: [
		    {name: "table1", key: ["colomn1"]}
		    {name: "table2", key: ["colomn1", "colomn2"]}
			// other tables to update ...
		  ]
		}

	对于增量更新的配置，非常的简单，您只需要配置update.tables节点，在其中添加您所需更新的表信息。其中name是表的名字，key是一个数组，代表能够唯一确定表中一行数据的列（字段），**需要注意的是，不要使用表的主键id字段**。

- DbUpdater

	com.fly.db.service.updater.DbUpdater是数据增量更新子系统的核心服务trait。您可以像下面这样使其工作：

		/**
		 * 增量更新数据库任务
		 */
		object UpdateDBTask extends DbUpdater with App {
		  updateTables()
		}

	运行上面的代码，增量更新子系统就开始工作了。

- 默认的数据库

	增量更新子系统，默认会将load-db的数据增量更新到update-db中，具体更新的表，就是您在application.conf中所配置的。

### 6、其他功能

DBFly的理念之一，是尽可能增加配置来实现功能，而不是增加代码。sql语句尽量集中在application.conf中，这样方便管理，减少维护成本。本着这样的原则，我们还提供了一些其他能力，以满足各种复杂场景的灵活处理。

- 单纯查询simple query

	我们在DbImporter中提供了如下方法：

		/**
		* 读取application.conf配置，查询数据库并返回结果
		* @param tableConfig 配置项
		* @param handle 对结果resultSet的处理函数
		* @param args select sql中的参数
		* @tparam B 对结果处理的返回类型
		* @return
		*/
		def simpleQueryData[B](tableConfig: Config, handle: ResultSet => B, args: List[String] = Nil) = {
		val desc = tableConfig.getString("description")
		logger.debug(s"Executing simple data query: $desc, args=$args")
		// 源数据库
		val stExec = autoGenSrcDB(tableConfig)
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
		* @param tableConfig 配置项
		* @param handle 对结果resultSet的处理函数
		* @param args select sql中的参数
		* @tparam B 对结果处理的返回类型
		* @return
		*/
		def simpleQueryData1[B](tableConfig: Config, handle: ResultSet => B, args: Map[String, String] = Map.empty) = {
		val desc = tableConfig.getString("description")
		logger.debug(s"Executing simple data query: $desc, args=$args")
		// 源数据库
		val stExec = autoGenSrcDB(tableConfig)
		stExec(st => {
		  // 从源中读取数据
		  val selectSql = tableConfig.getString("select")
		  val replacedSql = args.foldLeft(selectSql)((result, kv) => result.replaceAll(kv._1, kv._2))
		  val rs = st.executeQuery(replacedSql)
		  val result = handle(rs)
		  rs.close()
		  result
		})
		}

	simpleQueryData的目的是根据你的配置直接查询出你想要的数据并返回，假设你在application.conf中按如下方式配置：

		simple_query {
		  student {
		    description: "学生信息查询"
		    src_db: ${db.load_db}
		    select: """SELECT id,name,age,gender,score FROM student"""
		  }
		  // other simple query config...
		}

	那么在代码中，你可以如下方式使用simpleQueryData功能，其返回的数据studentInfo就是学生id和学生name的元组列表：

		val studentInfo = simpleQueryData("simple_query.student",
		    _.map(row => (row.getLong("id"), row.getString("name"))).toList)

	**占位符的使用**。在application.conf中配置的sql语句，如果您需要向sql语句中传递参数，那么您可以使用？问号作为占位符这样配置：

		simple_query {
		  student {
		    description: "学生信息查询"
		    src_db: ${db.load_db}
		    select: """SELECT age,gender,score FROM student
			WHERE id=? AND name='?'"""
		  }
		  // other simple query config...
		}

	那么您在具体的代码中，需要将占位符所代表的参数的具体值传递给simpleQueryData，参数使用列表来封装：

		val studentInfo = simpleQueryData("simple_query.student",
		    _.map(row => (row.getInt("age"), row.getString("name"))).toList, List(1, "xiaoming"))

	另一种替代方法是，使用具体的变量名，而不是使用？作为占位符:


		simple_query {
		  student {
		    description: "学生信息查询"
		    src_db: ${db.load_db}
		    select: """SELECT age,gender,score FROM student
			WHERE name='$name'"""
		  }
		  // other simple query config...
		}

	使用这种方式的话，您需要用Map来封装变量，并作为参数传递给simpleQueryData：

		val studentInfo = simpleQueryData("simple_query.student",
		    _.map(row => (row.getInt("age"), row.getString("name"))).toList, Map[String, String]("\\$name" -> "xiaoming")

- 单纯插入simple insert

	simple query常与simple insert配合使用，simple insert可以根据您的配置信息，将数据插入到目标数据库，它同样是在DbImporter里面提供的：

		/**
		* 读取application.conf中的配置，并将数据插入库
		* @param tableConfig 配置项
		* @param data 所需插入的数据
		*/
		def simpleInsertData(tableConfig: Config, data: Iterable[Map[String, Any]])

	方法的第一个参数是您的配置，第二个参数是所需插入的数据，是一个Map[String, Any]的迭代器。Map的Key代表插入的字段名，Value代表插入的字段值。在application中的配置非常简单：

		simple_insert {
		  student {
		    table: "student"
		    description: "导入学生信息"
		    src_db: ${db.db_load}
		  }
		  // other simple insert config...
		}

### 6、项目中的其他配置

- 日志打印配置：resources/logback.xml
- 项目构建及依赖配置：pom.xml

## Contact: ##

- Email: wjyheropk@163.com
