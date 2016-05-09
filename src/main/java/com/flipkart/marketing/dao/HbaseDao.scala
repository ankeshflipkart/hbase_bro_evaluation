package com.flipkart.marketing.dao

import java.io.IOException
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.flipkart.marketing.dao.HbaseDao.RowData
import com.google.common.base.CharMatcher
import org.apache.commons.codec.CharEncoding
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 *
 * @author durga.s
 * @version 11/18/15
 */
trait HbaseDao {


  @throws[IOException]
  def addRow(rowKey: String, data: RowData)(implicit hTableInterface: HTableInterface) = {

    val put: Put = new Put(rowKey.getBytes(CharEncoding.UTF_8))
    data.foreach { case (colFamily, v) =>
      v.foreach { case (colQualifier, d) =>
        put.add(colFamily.getBytes(CharEncoding.UTF_8), colQualifier.getBytes(CharEncoding.UTF_8), d)
      }
    }

    hTableInterface.put(put)
  }

//  @throws[IOException]
//  def removeRow(rowKey: String)(implicit hTableInterface: HTableInterface): Unit = {
//    val del = new Delete(rowKey.getUtf8Bytes)
//    hTableInterface.delete(del)
//  }
//
//  @throws[IOException]
//  def fetchRow(rowKey: String, colFamilies: List[String])(implicit hTableInterface: HTableInterface): RowData = {
//
//    val get: Get = new Get(rowKey.getBytes(CharEncoding.UTF_8))
//    colFamilies.foreach(cF => get.addFamily(cF.getBytes(CharEncoding.UTF_8)))
//
//    val rowResult = hTableInterface.get(get)
//    getRowData(rowResult, colFamilies)
//
//  }
//
//  @throws[IOException]
//  def fetchRowKeys(rowStartKeyPrefix: String, rowStopKeyPrefix: String, colFamilies: List[String], timeRange: Option[(Long, Long)] = None)(implicit hTableInterface: HTableInterface): List[String] = {
//    val scan = new Scan()
//    scan.setStartRow(rowStartKeyPrefix.getBytes(CharEncoding.UTF_8))
//    scan.setStopRow(rowStopKeyPrefix.getBytes(CharEncoding.UTF_8))
//
//    if (timeRange.isDefined)
//      scan.setTimeRange(timeRange.get._1, timeRange.get._2)
//
//    val filters = new FilterList()
//    filters.addFilter(new KeyOnlyFilter())
//    scan.setFilter(filters)
//
//    val resultScanner = hTableInterface.getScanner(scan)
//    val ri = resultScanner.iterator()
//
//    var results = ListBuffer[String]()
//    while (ri.hasNext) {
//      results += ri.next().getRow.getString
//    }
//
//    results.toList
//  }

//  /**
//   *
//   * @param rowStartKeyPrefix
//   * @param rowStopKeyPrefix
//   * @param colFamilies
//   * @param timeRange
//   * @param hTableInterface
//   * @return Map [Row ]
//   */
//  @throws[IOException]
//  def fetchRows(rowStartKeyPrefix: String, rowStopKeyPrefix: String, colFamilies: List[String], timeRange: Option[(Long, Long)] = None)(implicit hTableInterface: HTableInterface): Map[String, RowData] = {
//
//    val scan = new Scan()
//    scan.setStartRow(rowStartKeyPrefix.getBytes(CharEncoding.UTF_8))
//    scan.setStopRow(rowStopKeyPrefix.getBytes(CharEncoding.UTF_8))
//
//    if (timeRange.isDefined)
//      scan.setTimeRange(timeRange.get._1, timeRange.get._2)
//
//    val resultScanner = hTableInterface.getScanner(scan)
//    var rowMap = Map[String,RowData]()
//
//    val ri = resultScanner.iterator()
//    while (ri.hasNext) {
//      val riNext = ri.next()
//      val resultMap: RowData = getRowData(riNext, colFamilies)
//      rowMap += riNext.getRow.getString -> resultMap
//    }
//    resultScanner.close()
//    rowMap
//  }
//

  var counter : Int = 0
  @throws[IOException]
  def fetchMultiRows(rowKeys: String, colFamilies: List[String])(implicit hTableInterface: HTableInterface): Unit = {
    val gets = new Get(Bytes.toBytes(rowKeys))
    val rowResults = hTableInterface.get(gets)
    var map: Map[String, String] = Map()
    val fMap: util.NavigableMap[Array[Byte], Array[Byte]] = rowResults.getFamilyMap(Bytes.toBytes("d"))
    val iterator = fMap.keySet().iterator()
    while (iterator.hasNext) {
      val key = iterator.next()

      map += ((Bytes.toString(key)) -> Bytes.toString(fMap.get(key)))
    }
//  val global = map.get("global").get
  val check = CharMatcher.ASCII.matchesAllOf(map.get("global").get)
  check match {
    case true =>
    case false =>
      counter = counter + 1
      println(map.get("global").get.length + "counter = >" + counter)
  }


//    val mapper = new ObjectMapper()
//    val map_2 = mapper.readValue(map.get("global").get, util.Map[String, Any].getClass)
//  val index = global.indexOf("order_external_id")
//  val part_1 = global.substring(index + 22, index + 45)
//  print(rowKeys + "  " + part_1)

//  print(global)
//  (map.get("global"), new TypeReference[Map[String, String]](){})
  //  m.asInstanceOf[Map[String, Any]].get("ordered")
//  map.get("global").asInstanceOf[Map[Any,Any]].get("ordered").asInstanceOf[Map[Any,Any]].get("data").asInstanceOf[Map[Any,Any]].get("Order").asInstanceOf[Map[Any,Any]].get("order_external_id")
  }


//  private def getRowData(result: Result, colFamilies: List[String]): RowData = {
//    colFamilies.flatMap { cF =>
//      val optResult = result.getFamilyMap(cF.getBytes(CharEncoding.UTF_8))
//      Option(optResult).map(cFResult => {
//        val cQIterator = cFResult.keySet().iterator()
//        val cFData: ColumnData = cQIterator.asScala.map(colQualifier => colQualifier.toString -> cFResult.get(colQualifier)).toMap
//        cF -> cFData
//      })
//    }.toMap
//  }


}

object HbaseDao {



  type ColumnData = scala.collection.immutable.Map[String, Array[Byte]]
  // ColumnQualifer -> Data
  type RowData = scala.collection.immutable.Map[String, ColumnData] // ColumnFamily -> ColumnData

  val emptyRowData = Map[String, ColumnData]("d" -> Map("empty" -> Bytes.toBytes(1)))

  val objMapper = new ObjectMapper() with ScalaObjectMapper
  objMapper.registerModules(Seq(DefaultScalaModule): _*)

  implicit class stringHandyFunctions(val s: String) {
    def getUtf8Bytes = Bytes.toBytes(s)
  }

  implicit class longHandyFunctions(val l: Long) {
    def getBytes = Bytes.toBytes(l)
  }

  implicit class booleanHandyFunctions(val b: Boolean) {
    def getBytes = Bytes.toBytes(b)
  }

  implicit class byteArrayHandyFunctions(val b: Array[Byte]) {
    def getString = Bytes.toString(b)

    def getLong = Bytes.toLong(b)

    def getBoolean = Bytes.toBoolean(b)

    def getInt = Bytes.toInt(b)
  }

  implicit class mapKVHandyFunctions(val m: Map[String, Array[Byte]]) {
    def getS(key: String):String = m.get(key).map(_.getString).orNull

    def getB(key: String):Boolean = m.get(key).exists(_.getBoolean)

    def getL(key: String) = m.get(key).map(Bytes.toLong).getOrElse(null)

    def getKV(key: String):ObjectNode = m.get(key).map(_.getString).map(objMapper.readValue[ObjectNode]).orNull
  }

}

