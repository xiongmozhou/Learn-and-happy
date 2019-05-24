package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.bean.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

// 获取点击、下单和支付数量排名前 10 的品类
object Req1CategoryTop10Application {

    def main(args: Array[String]): Unit = {

        // TODO 4.1 获取hive表中的数据
        // 创建配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")

        // 构建SparkSession对象
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        // 引入隐式转换规则
        import sparkSession.implicits._

        val sql : StringBuilder = new StringBuilder("select * from user_visit_action where 1 = 1 ")

        val startDate = SparkmallUtil.getValueFromCondition("startDate")

        if ( SparkmallUtil.isNotEmptyString(startDate) ) {
            sql.append(" and action_time >= '" ).append(startDate).append("' ")
        }

        val endDate = SparkmallUtil.getValueFromCondition("endDate")

        if ( SparkmallUtil.isNotEmptyString(endDate) ) {
            sql.append(" and action_time <= '" ).append(endDate).append("' ")
        }

        // 从hive中查询数据
        sparkSession.sql("use " + SparkmallUtil.getValueFromConfig("hive.database"))
        val dataFrame: DataFrame = sparkSession.sql(sql.toString())

        // 将查询的结果转换为RDD进行统计分析
        val userVisitActionRDD: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd

        // TODO 4.2 配置累加器
        // 创建累加器
        val acc = new CategoryCountAccumulator
        // 注册累加器
        sparkSession.sparkContext.register(acc)

        // TODO 4.3 对原始数据进行遍历，实现累加（聚合）操作
        // 对数据进行累加处理
        userVisitActionRDD.foreach(action=>{
            if ( action.click_category_id != -1 ) {
                // category-click
                // 点击的场合
                acc.add(action.click_category_id + "_click")
            } else {

                if ( action.order_category_ids != null ) {
                    // 下单的场合
                    val ids = action.order_category_ids.split(",")

                    for ( id <- ids ) {
                        acc.add(id + "_order")
                    }
                } else {
                    if (action.pay_category_ids != null) {
                        // 支付的场合
                        val ids = action.pay_category_ids.split(",")

                        for ( id <- ids ) {
                            acc.add(id + "_pay")
                        }
                    }
                }
            }
        })

        // 获取累加器的执行结果
        val statResult : mutable.HashMap[String, Long] = acc.value

        // TODO 4.4 将累加的结果进行分组合并
        // （categoryid, Map( categoryId-clickclickCount, category-orderorderCount, category-paypayCount )）
        val groupMap: Map[String, mutable.HashMap[String, Long]] = statResult.groupBy {
            case (k, v) => { // 1, click
                k.split("_")(0)
            }
        }

        val taskId:String = UUID.randomUUID().toString

        // TODO 4.5 将合并的数据转换为一条数据
        // CategoryTop10(tasked, categoryid, clickcount, ordercount,paycount)
        val dataList: List[CategoryTop10] = groupMap.map {
            case (categoryid, map) => {
                CategoryTop10(
                    taskId,
                    categoryid,
                    map.getOrElse(categoryid + "_click", 0L),
                    map.getOrElse(categoryid + "_order", 0L),
                    map.getOrElse(categoryid + "_pay", 0L))
            }
        }.toList

        // TODO 4.6 将数据排序后取前10条
        val top10Data: List[CategoryTop10] = dataList.sortWith {
            case (left, right) => {
                if (left.clickCount > right.clickCount) {
                    true
                } else if (left.clickCount == right.clickCount) {
                    if (left.orderCount > right.orderCount) {
                        true
                    } else if (left.orderCount == right.orderCount) {
                        left.payCount > right.payCount
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }.take(10)

        // TODO 4.7 将结果保存到Mysql中

        val driverClass = SparkmallUtil.getValueFromConfig("jdbc.driver.class")
        val url = SparkmallUtil.getValueFromConfig("jdbc.url")
        val user = SparkmallUtil.getValueFromConfig("jdbc.user")
        val password = SparkmallUtil.getValueFromConfig("jdbc.password")

        Class.forName(driverClass)

        val connection: Connection = DriverManager.getConnection(url, user, password)
        val insertSQL = "insert into category_top10 values ( ?, ?, ?, ?, ? )"
        val statement: PreparedStatement = connection.prepareStatement(insertSQL)

        top10Data.foreach(data=>{
            statement.setObject(1, data.taskid)
            statement.setObject(2, data.categoryId)
            statement.setObject(3, data.clickCount)
            statement.setObject(4, data.orderCount)
            statement.setObject(5, data.payCount)

            statement.executeUpdate()
        })


        statement.close()
        connection.close()
        sparkSession.stop()


    }
}
case class CategoryTop10( taskid:String, categoryId:String, clickCount:Long, orderCount:Long, payCount:Long )
/**
  * 累加器 : (category-click， 100)， (category-order，50)， (category-pay, 10)
  */
class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

    var map = new mutable.HashMap[String, Long]

    // 是否为初始值
    override def isZero: Boolean = {
        map.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
        new CategoryCountAccumulator
    }

    // 重置累加器
    override def reset(): Unit = {
        map.clear()
    }

    // 累加数据
    override def add(key: String): Unit = {
        map(key) = map.getOrElse(key, 0L) + 1
    }

    /**
      * 合并数据
      * @param other
      */
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

        val map1 = map
        val map2 = other.value

        map = map1.foldLeft(map2){
            case (tempMap, (category, sumCount)) => {
                tempMap(category) = tempMap.getOrElse(category, 0L) + sumCount
                tempMap
            }
        }
    }

    /**
      * 累加器的值
      * @return
      */
    override def value: mutable.HashMap[String, Long] = map
}
