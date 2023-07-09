package com.loda.crypt

/**
 * @Author loda
 * @Date 2023/7/9 14:59
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
import org.apache.spark.sql.SparkSession
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.spark.sql.functions.udf

object MyCrypt2 {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.appName("IDEncryptionExample")
			.master("local[*]") // 在本地模式下运行
			.getOrCreate()

		// 示例原始数据
		val originalData = Seq(
			("John Doe", "1234567890"),
			("Jane Smith", "9876543210"),
			("Bob Johnson", "5678901234")
		)

		import spark.implicits._

		// 创建原始DataFrame
		val originalDF = originalData.toDF("name", "id_number")

		// 定义加密密钥
		val key = "0123456789abcdef"

		// 加密函数
		def encrypt(value: String): Array[Byte] = {
			val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
			val secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
			cipher.init(Cipher.ENCRYPT_MODE, secretKey)
			cipher.doFinal(value.getBytes("UTF-8"))
		}

		// 解密函数
		def decrypt(encrypted: Array[Byte]): String = {
			val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
			val secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
			cipher.init(Cipher.DECRYPT_MODE, secretKey)
			new String(cipher.doFinal(encrypted), "UTF-8")
		}

		// 注册UDF（用户定义函数）
		val encryptUDF = udf((value: String) => encrypt(value))
		val decryptUDF = udf((encrypted: Array[Byte]) => decrypt(encrypted))

		// 对身份证号码列进行加密
		val encryptedDF = originalDF.withColumn("encrypted_id", encryptUDF($"id_number"))

		// 对加密的身份证号码列进行解密
		val decryptedDF = encryptedDF.withColumn("decrypted_id", decryptUDF($"encrypted_id"))

		// 打印原始DataFrame
		println("Original DataFrame:")
		originalDF.show()

		// 打印加密后的DataFrame
		println("Encrypted DataFrame:")
		encryptedDF.show()

		// 打印解密后的DataFrame
		println("Decrypted DataFrame:")
		decryptedDF.show()

		spark.stop()
	}
}