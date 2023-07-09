package com.loda.util

import org.joda.time.LocalDateTime

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * @Author loda
 * @Date 2023/3/13 13:03
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object DateUtil {
	def getCurrentDate(): String = {
		LocalDate.now().toString
	}

	def getNextDate(): String = {
		LocalDate.now().plusDays(1).toString
	}

	def getLastDate(): String = {
		LocalDate.now().minusDays(1).toString
	}

	def getCurrentDate(curDate:String ): String = {
		val cur: LocalDate = parseDateString(curDate)
		cur.toString
	}

	def getNextDate(curDate:String): String = {
		val cur: LocalDate = parseDateString(curDate)
		cur.plusDays(1).toString
	}

	def getLastDate(curDate:String): String = {
		val cur: LocalDate = parseDateString(curDate)
		cur.minusDays(1).toString
	}

	def parseDateString(dateString: String) :LocalDate = {
		val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
		LocalDate.parse(dateString, formatter)
	}

	def main(args: Array[String]): Unit = {
//		println(LocalDate.now())
//		println(LocalDate.now().toString)
		getNextDate("2021-02-01")
		getLastDate("2021-02-01")
	}
}
