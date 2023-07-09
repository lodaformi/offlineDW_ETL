package com.loda.util

import org.apache.spark.sql.SparkSession

/**
 * @Author loda
 * @Date 2023/3/10 20:07
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object SparkSessionUtil {
	def getSparkSession(appName: String, master: String): SparkSession = {
		SparkSession.builder()
			.appName(appName)
			.enableHiveSupport()
			.config("spark.sql.shuffle.partitions", 1)
			.master(master)
			.getOrCreate()
	}
}
