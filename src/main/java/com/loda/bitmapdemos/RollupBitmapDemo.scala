package com.loda.bitmapdemos

import com.loda.udf.{BitMapAggr, BitmapUDAF}
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, DataInputStream}
/**
 * @Author loda
 * @Date 2023/3/16 16:06
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object RollupBitmapDemo {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.appName("rollup test")
			.master("local")
			.config("spark.sql.shuffle.partitions","1")
			.config("fs.defaultFS", "file:///")
			.getOrCreate()

		val logDF = spark.read.option("header", "true").option("inferSchema", "true").csv("data/rollup.csv")

//		logDF.show()
		logDF.createTempView("log")

		spark.udf.register("agr2bitmap",BitmapUDAF)

		val getCardinality = (b:Array[Byte]) => {
			val bitmap = RoaringBitmap.bitmapOf()

			val bain = new ByteArrayInputStream(b)
			val din = new DataInputStream(bain)
			bitmap.deserialize(din)
			bitmap.getCardinality
		}

		spark.udf.register("getCnt",getCardinality)

		val base = spark.sql(
			"""
				|
				|select
				| devicetype,province,isnew,
				| getCnt(agr2bitmap(guid)) as uv,
				| agr2bitmap(guid) as uv_bitmap,
				| sum(acc_tml) as acc_tml
				|from
				| log
				|group by devicetype,province,isnew
				|""".stripMargin)
//		base.show()

		base.createTempView("dim_base_cuboid")

		spark.udf.register("bm_aggr",BitMapAggr)

		spark.sql(
			"""
				|
				|select
				| province,
				| getCnt(bm_aggr(uv_bitmap)) as uv,
				| bm_aggr(uv_bitmap) as uv_bitmap,
				| sum(acc_tml) as acc_tml
				|from
				| dim_base_cuboid
				|group by province
				|
				|""".stripMargin).show()
	}
}
