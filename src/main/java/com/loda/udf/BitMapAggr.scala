package com.loda.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{BinaryType, DataType, StructField, StructType}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
/**
 * @Author loda
 * @Date 2023/3/16 15:24
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object BitMapAggr extends UserDefinedAggregateFunction{
	override def inputSchema: StructType = StructType(StructField("bitmap",BinaryType) :: Nil)

	override def bufferSchema: StructType = StructType(StructField("bitmapBuff",BinaryType) :: Nil)

	override def dataType: DataType = BinaryType

	override def deterministic: Boolean = true

	override def initialize(buffer: MutableAggregationBuffer): Unit = {
		val bitmap = RoaringBitmap.bitmapOf()
		buffer(0) = serBitmap(bitmap)
	}

	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		val bitmap1 = desBitmap(buffer.getAs[Array[Byte]](0))
		val bitmap2 = desBitmap(input.getAs[Array[Byte]](0))

		bitmap1.or(bitmap2)

		buffer.update(0,serBitmap(bitmap1))
	}

	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1,buffer2)

	override def evaluate(buffer: Row): Any = buffer.get(0)

	def serBitmap(bitmap:RoaringBitmap):Array[Byte]={
		val baout = new ByteArrayOutputStream()
		val dout = new DataOutputStream(baout)
		bitmap.serialize(dout)
		baout.toByteArray
	}

	def desBitmap(b:Array[Byte]):RoaringBitmap={
		val bitmap = RoaringBitmap.bitmapOf()

		val bain = new ByteArrayInputStream(b)
		val din = new DataInputStream(bain)
		bitmap.deserialize(din)
		bitmap
	}
}
