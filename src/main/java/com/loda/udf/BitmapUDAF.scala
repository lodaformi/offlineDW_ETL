package com.loda.udf

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, IntegerType, StructField, StructType}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
 * @Author loda
 * @Date 2023/3/16 15:13
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object BitmapUDAF extends UserDefinedAggregateFunction {
	//识别输入参数，定义输入row的schema
	override def inputSchema: StructType = StructType(StructField("guid", IntegerType) :: Nil)

	//识别buffer参数，定义buffer 的schema
	override def bufferSchema: StructType = StructType(StructField("guidBuff", BinaryType) :: Nil)

	//函数返回值类型
	override def dataType: DataType = BinaryType

	//校验一致性，如果有多个相同的数据输入，返回值不变
	override def deterministic: Boolean = false

	//初始化buffer
	override def initialize(buffer: MutableAggregationBuffer): Unit = {
		val bitmap = RoaringBitmap.bitmapOf()

		//序列化
		buffer(0) = serBitmap(bitmap)
	}

	//更新buffer
	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		val bitmap = desBitmap(buffer.getAs[Array[Byte]](0))

		bitmap.add(input.getInt(0))

		buffer(0) = serBitmap(bitmap)
	}

	//合并buffer，合并两个聚合缓冲区并将更新后的缓冲区值存在buffer1
	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		val bitmap1 = desBitmap(buffer1.getAs[Array[Byte]](0))
		val bitmap2 = desBitmap(buffer2.getAs[Array[Byte]](0))

		bitmap1.or(bitmap2)

		buffer1(0) = serBitmap(bitmap1)
	}

	//计算最终结果
	override def evaluate(buffer: Row): Any = buffer.get(0)

	def serBitmap(bitmap: RoaringBitmap): Array[Byte] = {
		val baout = new ByteArrayOutputStream()
		val dout = new DataOutputStream(baout)
		bitmap.serialize(dout)
		baout.toByteArray
	}

	def desBitmap(b: Array[Byte]): RoaringBitmap = {
		val bitmap = RoaringBitmap.bitmapOf()

		val bain = new ByteArrayInputStream(b)
		val din = new DataInputStream(bain)
		bitmap.deserialize(din)
		bitmap
	}
}
