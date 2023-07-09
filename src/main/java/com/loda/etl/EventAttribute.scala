package com.loda.etl
import com.loda.common.SparkProps
import com.loda.util.{DateUtil, SparkSessionUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
/**
 * @Author loda
 * @Date 2023/3/19 21:37
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object EventAttribute {
	def main(args: Array[String]): Unit = {
		val spark = SparkSessionUtil.getSparkSession("事件归因分析", SparkProps.LOCALMASTER)

		//g01,["1_e1","2_e2","3_e2","4_e9","5_e1","6_e3","7_e9","8_e3"]
		//g02,["1_e1","2_e2","3_e2","4_e9","5_e1","6_e3","7_e9","8_e3"]
		//g03,["1_e1","2_e2","3_e2","4_e9","5_e1","6_e3","7_e9","8_e3"]

		import spark.implicits._

		val events:DataFrame = spark.sql(
			s"""
				 |
				 |select
				 | guid,
				 | sort_array(collect_list(concat_ws('_',timestamp,eventid))) events
				 |
				 |from
				 | dwd.dwd_app_event_detail
				 |where
				 | dt='2021-02-01'
				 |and
				 | (
				 |   eventid='fetchCoupon'
				 |   or
				 |   eventid = 'goodsView'
				 |   or
				 |   eventid = 'addCart'
				 |   or
				 |   eventid = 'adShow'
				 | )
				 | group by guid
				 |
				 |""".stripMargin)

		//根据目标事件发生的次数进行分段
		//g01,["1_e1","2_e2","3_e2","4_e9","5_e1","6_e3","7_e9","8_e3"]
		//g01,["1_e1","2_e2","3_e2"]
		//g01,["5_e1","6_e3"]

		val rdd:RDD[(Long,String,String)] = events.rdd.flatMap(row => {
			val guid: Long = row.getLong(0)

			//dataFrame中的数组（array<String>），在API中对应的是mutable.WrappedArray[String] 类型
			val events: mutable.WrappedArray[String] = row.getAs[mutable.WrappedArray[String]]("events")

			//先将事件列表变形，只留下每一个事件的eventid，将整个列表变成一个整体字符串
			val str: String = events.map(s => s.split("_")(1)).mkString(",")

			//按照目标事件名切割字符串
			//["a,b,c,"] ["a,b,d,",] ...,,,...,[",",""]
			val eventsStrArray = str.split("fetchCoupon")
				.filter(s => StringUtils.isNotBlank(s.replaceAll(",", "")))

			eventsStrArray.map(str => (guid, "fetchCoupon", str))
		})
//		rdd.toDF().show()
//		|  1|fetchCoupon|            addCart,|
//		|  1|fetchCoupon|   ,addCart,addCart,|
//		|  1|fetchCoupon|,addCart,addCart,...|
//		|  1|fetchCoupon|,adShow,adShow,go...|

		//(g01,"x",",a,b,c,d")
		//计算归因权重
		val resRdd: RDD[(String, String, Long, String, String, Double)] = linearAttribute(rdd)

		val res = resRdd.toDF("model", "strategy", "guid", "dest_event", "attr_event", "weight")

		res.createTempView("tmp")

		spark.sql(
			s"""
				 |
				 |insert overwrite table dws.dws_event_attribute_day partition(dt='2021-02-01')
				 |select
				 | model,
				 | strategy,
				 | guid,
				 | dest_event,
				 | attr_event,
				 | weight
				 |from
				 | tmp
				 |
				 |""".stripMargin)

		spark.close()
	}


	/**
	 * 线性归因策略
	 * @param rdd
	 * @return
	 */
	def linearAttribute(rdd:RDD[(Long,String,String)]):RDD[(String,String,Long,String,String,Double)] ={
		rdd.flatMap(tp =>{
			val guid = tp._1
			val destEvent = tp._2
			val events = tp._3
			val eventArray = events.split(",").filter(s => StringUtils.isNotBlank(s))

			val weight = 100.0 / eventArray.size

			eventArray.map(e => ("优惠券获取归因分析","线性归因",guid,destEvent,e,weight))
		})
	}
}
