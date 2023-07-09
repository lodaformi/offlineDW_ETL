package com.loda.geo

import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * @Author loda
 * @Date 2023/3/8 17:24
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Ip2RegionTest {
	def main(args: Array[String]): Unit = {
		val config = new DbConfig
		val searcher = new DbSearcher(config, "data/ip2region.db")

		val block: DataBlock = searcher.memorySearch("39.99.177.94")
		println(block)
	}
}
