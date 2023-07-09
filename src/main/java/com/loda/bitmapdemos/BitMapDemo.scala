package com.loda.bitmapdemos

import org.roaringbitmap.RoaringBitmap

/**
 * @Author loda
 * @Date 2023/3/16 15:08
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object BitMapDemo {
	def main(args: Array[String]): Unit = {
		val bm1: RoaringBitmap = RoaringBitmap.bitmapOf(1, 2, 3)
		val bm2: RoaringBitmap = RoaringBitmap.bitmapOf(5,3)
		bm1.or(bm2)
		println(bm1.getCardinality)
	}
}
