package com.loda.etl

import com.loda.common.SparkProps
import com.loda.util.{DateUtil, SparkSessionUtil}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author loda
 * @Date 2023/3/10 20:16
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object DeviceAccountBind {
	def main(args: Array[String]): Unit = {

		var cur_date: String = DateUtil.getCurrentDate("2021-02-06")
		var pre_date: String = DateUtil.getLastDate(cur_date)

		if (args.length == 2) {
			pre_date = args(0)
			cur_date = args(1)
		}

		val spark: SparkSession = SparkSessionUtil.getSparkSession("设备账号绑定计算任务", SparkProps.LOCALMASTER)

		//读取当天日志
		val curDayLog: DataFrame = spark.read.table("ods.ods_app_event_log")
			.where(s" dt='${cur_date}' and eventid is not null")
			.selectExpr("deviceid", "if(account='', 'null', account) account",
				"sessionid", "timestamp")

//		curDayLog.show(20)
		curDayLog.createTempView("curDayLog")
		//根据deviceid, account聚合当天的日志，得出每一种“设备-账号”的会话数，并转换为score
		val curDayCombineSessionCnt: DataFrame = spark.sql(
			"""
				|select
				| deviceid,
				| account,
				| 100 * count(distinct sessionid) as score,
				| max(timestamp) as last_login
				| from curDayLog
				|group by deviceid, account
				|
				|""".stripMargin)
//		curDayCombineSessionCnt.show(10)
		curDayCombineSessionCnt.createTempView("cur")

		//3获取前一日的绑定评分表
		val preTable: Dataset[Row] = spark.read.table("dws.dws_device_account_bind_score")
			.where(s" dt='${pre_date}'")
//		preTable.show()
			preTable.createTempView("pre")

		//4当日的（设备-账号）聚合结果full join前日的绑定评分表
		// null值不能直接判断相等
		//将查询出来，并算好的数据使用hive写入dws层的dws_device_account_bind_score表中
		// 为防止重复写入dws层，最好使用insert overwrite
		/*1.T-1日（绑定评分表）出现的组合，但是T日（日志）没有出现，做score衰减
		2.T-1日（绑定评分表）出现的组合，但是T日（日志）也出现，做score累加
		3.T-1日（绑定评分表）没出现的组合，但是T日（日志）出现，取T日的score*/
		val curBindScoreResult: DataFrame = spark.sql(
			s"""
				|insert into table dws.dws_device_account_bind_score partition(dt= '${cur_date}')
				|select
				| nvl(cur.deviceid, pre.deviceid) deviceid,
				| nvl(cur.account, pre.account) account,
				| case
				|   when cur.score is null and pre.score is not null then cur.score * 0.6
				|   when cur.score is not null and pre.score is not null then cur.score + pre.score
				|   when cur.score is not null and pre.score is null then cur.score
				|   end as score,
				| nvl(cur.last_login, pre.last_login) last_login
				|from
				|   cur full join pre
				|on
				|   cur.deviceid = pre.deviceid
				|   and if(cur.account=null,'', cur.account) = if(pre.account=null, '', pre.account)
				|""".stripMargin)
//				curBindScoreResult.show(20)

		spark.close()
	}
}
