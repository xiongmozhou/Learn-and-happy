package com.atguigu.bigdata.sparkmall.common.util

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Date, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 电商项目工具类
  */
object SparkmallUtil {


    def main(args: Array[String]): Unit = {

        //println(getValueFromConfig("jdbc.url"));
       // println(getValueFromCondition("endDate"))

        println(parseStringDateFromTs(formatString="yyyy-MM-dd"))

    }

    /**
      * 将时间字符串转换为日期对象
      * @return
      */
    def formatDateFromString( time:String, formatString : String = "yyyy-MM-dd HH:mm:ss" ): Date = {
        val format = new SimpleDateFormat(formatString)
        format.parse(time)
    }

    /**
      * 将时间戳转换为日期字符串
      */
    def parseStringDateFromTs( ts : Long = System.currentTimeMillis, formatString : String = "yyyy-MM-dd HH:mm:ss" ): String = {
        val format = new SimpleDateFormat(formatString)
        format.format(new Date(ts))
    }

    /**
      * 判断字符串是否非空，true,非空，false，空
      * @param s
      * @return
      */
    def isNotEmptyString( s : String ): Boolean = {
        s != null && !"".equals(s.trim)
    }

    def getValueFromCondition(key : String): String = {
        val condition: String = getValueFromProperties("condition", "condition.params.json")
        // 将JSON字符串进行转换
        val jsonObj : JSONObject = JSON.parseObject(condition)

        jsonObj.getString(key)
    }

    def getValueFromConfig(key : String): String = {
        getValueFromProperties("config", key);
    }

    /**
      * 读取配置文件
      * @param fileName 配置文件名称
      * @param key 配置key
      * @return
      */
    def getValueFromProperties(fileName : String, key : String): String = {
        /* 读取配置文件
        val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName)

        val properties = new java.util.Properties()
        properties.load(stream)
        properties.getProperty(key)
        */

        // 使用第三方的组件，读取配置文件
        /*
new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
        .configure(new Parameters().properties().setFileName(propertiesName)).getConfiguration
    }

         */

        // 使用国际化（i18n）组件读取配置文件，只能读取properties文件
        val bundle = ResourceBundle.getBundle(fileName);
        bundle.getString(key)

    }
}
