package com.loda.util

/**
 * @Author loda
 * @Date 2023/3/8 20:04
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Ip2Decimal {
	def main(args: Array[String]): Unit = {
		val ip = "120.24.78.129"
		val ipList: Array[String] = ip.split("\\.")
		ipList.foreach(println)

//		val array: Array[Int] = (1 to 10).toArray
//		array.foreach(println)
//		for (i <- 1 to ipList.length)
//			println(ipList(i))
	}
}
