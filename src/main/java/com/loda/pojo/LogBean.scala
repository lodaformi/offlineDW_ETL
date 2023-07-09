package com.loda.pojo

/**
 * @Author loda
 * @Date 2023/3/11 11:52
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
case class LogBean(
	                  account: String,
	                  appId: String,
	                  appVersion: String,
	                  carrier: String,
	                  deviceId: String,
	                  eventId: String,
	                  ip: String,
	                  latitude: Double,
	                  longitude: Double,
	                  netType: String,
	                  osName: String,
	                  osVersion: String,
	                  properties: Map[String, String],
	                  resolution: String,
	                  sessionId: String,
	                  timeStamp: BigInt,
	                  //ods表之外的数据
	                  var splitedSessionId: String = "",
	                  var filledAccount: String = "",
	                  var province: String = "",
	                  var city: String = "",
	                  var region: String = "",
	                  var guid: BigInt = -1,
	                  var isnew: Int = 0
                  )
