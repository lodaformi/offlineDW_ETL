package com.loda.etl

import ch.hsr.geohash.GeoHash
import com.loda.common.{RedisProps, SparkProps}
import com.loda.pojo.LogBean
import com.loda.util.{DateUtil, SparkSessionUtil}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.UUID

/**
 * @Author loda
 * @Date 2023/3/11 10:42
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object OdsApp2DwdApp {
	//判断新老用户时使用
	var lastMaxGuid = 0L

	def main(args: Array[String]): Unit = {
		if (args.length < 5) {
			println(
				"""
					|usage:
					|ages(0) : redis hostname
					|ages(1) : redis port
					|ages(2) : redis auth
					|args(3): log current day like '2021-02-01'
					|args(4): log next day like '2021-02-02'
					|
					|""".stripMargin)
			sys.exit(1)
		}
		var redis_host = RedisProps.HOSTNAME
		var redis_port = RedisProps.PORT
		var redis_auth = RedisProps.auth

		var pre_date = DateUtil.getLastDate("2021-02-02")
		var cur_date = DateUtil.getCurrentDate("2021-02-02")

		if (args.length >= 5) {
			redis_host = args(0)
			redis_port = args(1).toInt
			redis_auth = args(2)
			pre_date = args(3)
			cur_date = args(4)
		}

		val jedis1 = new Jedis(redis_host, redis_port)
		jedis1.auth(redis_auth)
		//获取前一日最大Guid
		val cnt: String = jedis1.get("guid_cnt")
		if (cnt != null) lastMaxGuid = cnt.toLong
		jedis1.close()

		val spark: SparkSession = SparkSessionUtil.getSparkSession("App日志ODS层数据ETL到DWD层", SparkProps.YARNMASTER)

		//一:清洗过滤
		val filtered: DataFrame = dataClear(spark, pre_date, cur_date)

		//二：数据规范化处理
		val logBean: Dataset[LogBean] = dataFormal(spark, filtered)

		//三：Session分割（30min）
		/*u01,s01,ev1,t1,ss01
			u01,s01,ev1,t2,ss01
			u01,s01,ev1,t3,ss01
			u01,s01,ev1,t7,ss02
			u01,s01,ev1,t8,ss02

			u02,s02,ev1,t3
			u02,s02,ev1,t5
			u02,s02,ev1,t8
			u02,s02,ev1,t9
			u02,s02,ev1,t12*/
		val splitedSession: Dataset[LogBean] = sessionSplit(spark, logBean)

		//四：数据集成（GPS/IP）
		val areaed: Dataset[LogBean] = dataIntegrate(spark, splitedSession)

		//五：回填GUID和标识新老访客
		val res: Dataset[LogBean] = fillAccountAndGuidAndIsNewAccount(spark, areaed, cur_date, redis_host, redis_port, redis_auth)

		//六：保存结果
		dataSave(spark, res, cur_date)
	}

	/**
	 * 清晰过滤
	 *
	 * @param spark
	 * @param pre_date
	 * @param cur_date
	 * @return
	 */
	def dataClear(spark: SparkSession, pre_date: String, cur_date: String): DataFrame = {
		//从ods层读取前一天日志数据
		val curLog: Dataset[Row] = spark.read.table("ods.ods_app_event_log")
			.where(s" dt='${pre_date}'")

		//1. 去除json数据体中的废弃字段（前端开发人员在埋点设计方案变更后遗留的无用字段）
		//2. 过滤掉json格式不正确的（脏数据）
		//3. 过滤掉日志中缺少关键字段（deviceid/properties/eventid/sessionid 缺任何一个都不行）的记录！
		//4. 过滤掉日志中不符合时间段的记录（由于app上报日志可能的延迟，有数据延迟到达）
		//5. 对于web端日志，过滤爬虫请求数据（通过useragent标识来分析

		//使用udf机制自定义辅助判断字段函数
		import org.apache.spark.sql.functions._
		val isNotBlank: UserDefinedFunction = udf((s: String) => {
			StringUtils.isNotBlank(s)
		})

		import spark.implicits._
		//3. 过滤掉日志中缺少关键字段（deviceid/properties/eventid/sessionid 缺任何一个都不行）的记录！
		val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		val startTime: Long = format.parse(s"${pre_date} 00:00:00").getTime
		val endTime: Long = format.parse(s"${cur_date} 00:00:00").getTime

		//		val filtered: Dataset[Row] =
		curLog.where(isNotBlank($"deviceid") and
			'properties.isNotNull and
			isNotBlank(col("eventid")) and
			isNotBlank(curLog("sessionid")))
			//日志产生是按照event time生成的，即使延迟到达，有水位线的设置，也能保证一定的准确性，
			//小括号中的话语有点问题？？
			//4. 过滤掉日志中不符合时间段的记录（由于app上报日志可能的延迟，有数据延迟到达）
			.where(s" timestamp >= ${startTime} and timestamp < ${endTime}")

		//查看结果是否正确
		//filtered.show()
		//		filtered.createTempView("data")
	}

	/**
	 * 数据规范化
	 *
	 * @param spark
	 * @param filtered
	 */
	def dataFormal(spark: SparkSession, filtered: DataFrame): Dataset[LogBean] = {
		import spark.implicits._
		//将account的空值置为null，null在后续进行一些计算时不会包括在里面，比如数值运算
		filtered.createTempView("data")
		val standard: DataFrame = spark.sql(
			"""
				|select
				| if(account='', null,account) as account,
				| appId      ,
				| appVersion ,
				| carrier    ,
				| deviceId   ,
				| eventId    ,
				| ip         ,
				| latitude   ,
				| longitude  ,
				| netType    ,
				| osName     ,
				| osVersion  ,
				| properties ,
				| resolution ,
				| sessionId  ,
				| timeStamp ,
				| null as SplitedsessionId,
				| null as filledAccount,
				| null as province,
				| null as city,
				| null as region,
				| -1 as guid,
				| 0 as isNew
				|
				|from
				| data
				|""".stripMargin)

		//		standard.show()
		//转换成对象方便处理字段
		//		val logBean: Dataset[LogBean] = standard.as[LogBean]
		//		logBean.show()
		standard.as[LogBean]
	}

	/**
	 * 会话分割
	 *
	 * @param spark
	 * @param logBean
	 */
	def sessionSplit(spark: SparkSession, logBean: Dataset[LogBean]): Dataset[LogBean] = {
		//使用
		import spark.implicits._
		//		val splitedSession: Dataset[LogBean] =
		logBean.rdd.groupBy(bean => bean.sessionId)
			.flatMap(tp => {
				val iter = tp._2
				val list = iter.toList.sortBy(bean => bean.timeStamp)
				var tmp = UUID.randomUUID().toString
				for (i <- 0 until list.size) {
					list(i).splitedSessionId = tmp
					if (i < list.size - 1 &&
						(list(i + 1).timeStamp - list(i).timeStamp) > 30 * 60 * 1000)
						tmp = UUID.randomUUID().toString
				}
				list
			}).toDS()
		//		splitedSession.show()
	}

	/**
	 * 数据集成
	 *
	 * @param spark
	 * @param splitedSession
	 * @param start_date
	 */
	def dataIntegrate(spark: SparkSession, splitedSession: Dataset[LogBean]): Dataset[LogBean] = {
		import spark.implicits._
		//读出dim.dim_area_dict表中的信息，这是一张小表，
		// 广播出去，利用map端的join（在查询之前就已经将两张表的数据准备好了）节省空间，效率更高
		val areaDict: Dataset[Row] = spark.read.table("dim.dim_area_dict")
			.where("geohash is not null and geohash != ''")

		val gpsDictMap = areaDict.rdd.map({ case Row(geohash: String, province: String, city: String, region: String)
		=> (geohash, (province, city, region))
		}).collectAsMap()
		//广播数据
		val bc = spark.sparkContext.broadcast(gpsDictMap)

		//读取ip2region库文件
		val configuration = new Configuration()
		val fs = FileSystem.get(configuration)
		val path = new Path("/yjx/dict/ip2region.db")
		val inputStream = fs.open(path)
		val len = fs.getFileStatus(path).getLen
		val array = new Array[Byte](len.toInt)
		IOUtils.readFully(inputStream, array)
		IOUtils.closeQuietly(inputStream)

		val bc2 = spark.sparkContext.broadcast(array)

		//map 和 mapPartitions的区别??
		//		val areaed: Dataset[LogBean] =
		splitedSession.mapPartitions(iter => {
			//读取广播数据中的gps-area-dict
			val gpsDict = bc.value

			//读取广播中的数据（ip2region）
			val ip2regionBytes = bc2.value
			val config = new DbConfig()
			val searcher = new DbSearcher(config, ip2regionBytes)

			iter.map(bean => {
				var flag = false;
				try {
					val latitude = bean.latitude
					val longitude = bean.longitude

					val geostr = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 6)
					if (gpsDict.contains(geostr)) {
						flag = true;
						val areaInfo = gpsDict.getOrElse(geostr, ("", "", ""))
						bean.province = areaInfo._1
						bean.city = areaInfo._2
						bean.region = areaInfo._3
					}
				} catch {
					case exception: Exception => exception.printStackTrace()
				}

				//如果gps信息读取失败，从ip中获取位置信息
				if (!flag) {
					val block = searcher.memorySearch(bean.ip)
					val strings = block.getRegion().split("\\|")
					if (strings.size >= 5 && StringUtils.isNotBlank(strings(2)) && StringUtils.isNotBlank(strings(3))) {
						bean.province = strings(2)
						bean.city = strings(3)
					}
				}
				bean
			})
		})
		//		areaed.show()
	}

	/**
	 * fillAccount和全局guid和新老用户标记
	 *
	 * @param spark
	 * @param areaed
	 * @param cur_date
	 * @param redis_host
	 * @param redis_port
	 * @param redis_auth
	 * @return
	 */
	def fillAccountAndGuidAndIsNewAccount(spark: SparkSession, areaed: Dataset[LogBean], cur_date: String,
	                                      redis_host: String, redis_port: Int, redis_auth: String): Dataset[LogBean] = {
		import spark.implicits._
		//回填账号
		areaed.createTempView("areaed")
		val anonymousFilled = spark.sql(
			s"""
				 |select
				 | areaed.account   ,
				 | areaed.appId       ,
				 | areaed.appVersion  ,
				 | areaed.carrier     ,
				 | areaed.deviceId    ,
				 | areaed.eventId     ,
				 | areaed.ip          ,
				 | areaed.latitude    ,
				 | areaed.longitude   ,
				 | areaed.netType     ,
				 | areaed.osName      ,
				 | areaed.osVersion   ,
				 | areaed.properties  ,
				 | areaed.resolution  ,
				 | areaed.sessionId   ,
				 | areaed.timeStamp ,
				 | areaed.SplitedsessionId,
				 | nvl(areaed.account, o2.account) filledAccount,
				 | areaed.province,
				 | areaed.city,
				 | areaed.region,
				 | areaed.guid,
				 | areaed.isNew
				 |from
				 |   areaed
				 |left join
				 |  (select
				 |     deviceid,
				 |     account
				 |    from
				 |      (select
				 |          deviceid,
				 |          account,
				 |          row_number() over(partition by deviceid order by score desc, last_login desc) as r
				 |        from
				 |          dws.dws_device_account_bind_score
				 |        where
				 |          dt='${cur_date}' and account is not null
				 |      ) o1
				 |      where r=1
				 |    )o2
				 | on areaed.deviceid = o2.deviceid
				 |
				 |""".stripMargin)
		val ds = anonymousFilled.as[LogBean]

		//		val res: Dataset[LogBean] =
		ds.mapPartitions(iter => {
			val jedis = new Jedis(redis_host, redis_port)
			jedis.auth(redis_auth)

			var guidStr: String = null
			iter.map(bean => {
				try {
					guidStr = jedis.get(bean.filledAccount)
				} catch {
					case exception: Exception =>
				}
				//上面可能正常结束，即bean.filledAccount不是null
				//也可能不正常结束，即bean.filledAccount为null，进入到else分支
				if (bean.filledAccount != null && guidStr != null) {
					bean.guid = guidStr.toLong
				} else {
					//如果用户账号找不到，则用deviceid去取
					guidStr = jedis.get(bean.deviceId)
					if (guidStr != null) {
						bean.guid = guidStr.toLong

						if (bean.filledAccount != null) {
							jedis.del(bean.deviceId)
							jedis.set(bean.filledAccount, guidStr)
						}
					} else {
						//用户ID和deviceid都查不到，包括用户ID为空的情况
						//用计数器获取一个新的guid
						val newGuid = jedis.incr("guid_cnt")

						//将结果插入到redis
						val key = if (bean.filledAccount == null) bean.deviceId else bean.filledAccount

						//将guid添加到redis
						jedis.set(key, newGuid + "")
						bean.guid = newGuid.toLong
					}
				}
				//五：新老访客标记
				//标记新老访客，新用户：1 老用户：不用动（0）
				if (bean.guid > lastMaxGuid) bean.isnew = 1
				bean
			})
		})
	}

	/**
	 * 保存结果
	 *
	 * @param spark
	 * @param cur_date
	 */
	def dataSave(spark: SparkSession, res: Dataset[LogBean], cur_date: String): Unit = {
		res.createTempView("res")
		spark.sql(
			s"""
				 | insert into table dwd.dwd_app_event_detail partition(dt='${cur_date}')
				 | select
				 | account   ,
				 | appId       ,
				 | appVersion  ,
				 | carrier     ,
				 | deviceId    ,
				 | eventId     ,
				 | ip          ,
				 | latitude    ,
				 | longitude   ,
				 | netType     ,
				 | osName      ,
				 | osVersion   ,
				 | properties  ,
				 | resolution  ,
				 | sessionId   ,
				 | timeStamp ,
				 | splitedSessionId,
				 | filledAccount,
				 | province,
				 | city,
				 | region,
				 | guid,
				 | isnew
				 |
				 |from
				 | res
				 |""".stripMargin)
	}
}
