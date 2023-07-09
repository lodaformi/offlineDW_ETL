package com.loda.util
import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @Author loda
 * @Date 2023/3/8 17:35
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object AreaDictUtil {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.appName("地理位置字典加工")
			.master("local[*]")
			.enableHiveSupport()
			.getOrCreate()

		val props = new Properties()

		props.put("user","root")
		props.put("password","123456")

		val df = spark.read.jdbc("jdbc:mysql://node01:3306/yjx-shop?useUnicode=true&characterEncoding=utf-8", "t_md_areas", props)
//		df.show(10)

		df.createTempView("area")

		val gps2geo = (lat:Double,lng:Double) => {
			GeoHash.withCharacterPrecision(lat, lng, 6).toBase32
		}

		spark.udf.register("geo",gps2geo)

//		val sql =
//			"""
//				|select
//				|geo(a1.bd09_lng, a1.bd09_lat) geohash,
//				|a1.areaname provice,
//				|a2.areaname city,
//				|a3.areaname region,
//				|from
//				|area a4 join area a3 on a4.parentid = a3.id and a4.level = 4
//				|join area a2 on a3.parentid = a2.id
//				|join area a1 on a2.parentid = a1.id
//				|""".stripMargin

//				spark.sql(
//					"""
//						|
//						|select
//						| geo(l4.bd09_lat,l4.bd09_lng) as geohash,
//						| l1.areaname province,
//						| l2.areaname city,
//						| l3.areaname region
//						|from
//						| area l4 join area l3 on l4.level = 4 and l4.parentid = l3.id
//						|         join area l2 on l3.parentid = l2.id
//						|         join area l1 on l2.parentid = l1.id
//						|
//						|""".stripMargin).show(20)

		spark.sql(
			"""
				|insert overwrite table dim.dim_area_dict
				|select
				| geo(l4.bd09_lat,l4.bd09_lng) as geohash,
				| l1.areaname province,
				| l2.areaname city,
				| l3.areaname region
				|from
				| area l4 join area l3 on l4.level = 4 and l4.parentid = l3.id
				|         join area l2 on l3.parentid = l2.id
				|         join area l1 on l2.parentid = l1.id
				|""".stripMargin)

		spark.close()
	}
}
