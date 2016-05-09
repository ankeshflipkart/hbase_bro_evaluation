package com.flipkart.marketing.benchrunners

import java.io.StringWriter
import java.util.Properties

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.flipkart.marketing.commons.behaviours.Bench
import com.flipkart.marketing.commons.connections.ConnectionProvider
import com.flipkart.marketing.commons.factories.HTableFactoryWrapper
import com.flipkart.marketing.dao.HbaseDao
import com.typesafe.config.ConfigFactory
import com.yammer.metrics.core.{Counter, MetricsRegistry}
import org.apache.hadoop.hbase.client.{Put, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.parsing.json._



/**
 *
 *
 * @author durga.s
 * @version 11/18/15
 */
class ThreadWorker extends HbaseDao with Bench with Runnable{

  var hConnectionHelper = getHBaseConnHelper
  var threadNumber = 0
  var exceptionCounter: Counter = null
  var registry: MetricsRegistry = null
  var maxThreadCount: Int = 0
  //var rowKeys = List("ABC1YT")
  var rowKeys = List("ABCNYTGFEE_HIJKMNOP" , "DEFNKRJBTNSRA_EAJKNRSV","GHINAEFJKNR_JRKNA","JKLNASDE_AEJKNV","MOPNRMLKBSAVE_EAVAV","PQRNJNGSSLBS_VRSJKK","RSTNTKBLSQB_RSBSR","UVWNJRSV_RSBJK","XYZNJRNSBE_BRSJ","OKLNYTGFEE_HIJKMNO")

  def this( hConnection: HTableFactoryWrapper , threadNumber : Int, maxThreadCount: Int, _eCounter: Counter, _registry : MetricsRegistry) {
    this()
    this.hConnectionHelper = hConnection
    this.threadNumber = threadNumber
    this.exceptionCounter = _eCounter
    this.registry = _registry
    this.maxThreadCount = maxThreadCount
  }
  val tblName = "w3_bro_EntityState"

  override def run(): Unit = {
//    val fullScanTimer = registry.newTimer(this.getClass, "HBase-Full-Scan-Timer")
//    val getNextTimer = registry.newTimer(this.getClass, "HBase-Get-Next-Timer")
//    val queryTimer = registry.newTimer(this.getClass, "HBase-Query-Timer")
    implicit var h = hConnectionHelper.getTableInterface(tblName)
//
//    for (index <- 0 to rowKeys.size)
//    {
//      for (i <- 1 to maxThreadCount) {
//        val starRowKey = rowKeys(index).replace("N", i.toString)
//        val endRowKey = starRowKey + "~"
//        val scan: Scan = new Scan(Bytes.toBytes(starRowKey), Bytes.toBytes(endRowKey))
//        //We've to set the prefetch count, and make sure this doesn't populate the blockCache
//        scan.setCaching(5000)
//        scan.setCacheBlocks(false)
//
//        val ctxRangeQuery = queryTimer.time()
//        val rs: ResultScanner = h.getScanner(scan)
//        ctxRangeQuery.stop()
//
//        val iterator = rs.iterator()
//        var hasNextValue = iterator.hasNext
//        var count = 0
//
//        val ctxFullScan  = fullScanTimer.time()
//
//        while (hasNextValue) {
//          val ctxGetNext = getNextTimer.time()
//          try {
//            iterator.next()
//            hasNextValue = iterator.hasNext
//            ctxGetNext.stop()
//          } catch {
//            case e: Exception =>
//              ctxGetNext.stop()
//              println("Exception encountered during iteration" + e.toString)
//              exceptionCounter.inc()
//          }
//          count += 1
//        }
//        ctxFullScan.stop()
////        println(s"Thread ${Thread.currentThread().getName}: Completed RangeScan for $starRowKey")
//      }
//    }


    val starRowKey = "SMA3JH2L_ACC14004228367242967"
    val endRowKey = "SMA3JH2L_ACC773BC0D7D3CE4F3E9F9BBC6199DDA616R"
    val scan: Scan = new Scan(Bytes.toBytes(starRowKey), Bytes.toBytes(endRowKey))
    //We've to set the prefetch count, and make sure this doesn't populate the blockCache
//    scan.setCaching(5000)
//    scan.setCacheBlocks(false)

    val rs: ResultScanner = h.getScanner(scan)

    val iterator = rs.iterator()
    var hasNextValue = iterator.hasNext
    var count = 0

    var i=0
    while (hasNextValue) {
      try {
        val t =iterator.next()
        val kv = t.getColumnLatest(Bytes.toBytes("d"), Bytes.toBytes("global"))
      //  println(Bytes.toString(kv.getValue()))
        val test = Bytes.toString(kv.getValue)
//        test.asInstanceOf[List]
        val parsed = JSON.parseFull(test)
        val le = parsed.get.asInstanceOf[Map[String,Any]].getOrElse("browse","").asInstanceOf[List[Any]]
        var data  = List[Map[String,Any]]()
        if(le.size > 40) {
          for(index <- 1 to 40) {
            data = data ++ List(le(index).asInstanceOf[Map[String,Any]])
          }

          var map = parsed.get.asInstanceOf[Map[String,Any]]
          map += ("browse" -> data)

          val out : StringWriter  = new StringWriter()
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          mapper.configure(SerializationFeature.INDENT_OUTPUT, true)
          mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true)
          val writer = new StringWriter
          mapper.writeValue(writer, map)
          val json = writer.toString
//          val jsonMap = new JSONObject(map)
//          val jsonMap = scala.util.parsing.json.JSONObject(map)
          val cellput: Put = new Put(t.getRow)

          cellput.add(Bytes.toBytes("d"), Bytes.toBytes("global"),Bytes.toBytes(json))

          h.put(cellput)
          println(s"Corrected row ${Bytes.toString(t.getRow)}")
        }



//        println(map)
//        val check = CharMatcher.ASCII.matchesAllOf(Bytes.toString(kv.getValue))
//        check match {
//          case true =>
//          case false =>
//            count = count + 1
//            println(Bytes.toString(kv.getValue).length + " counter => " + count + " AccId - " + Bytes.toString(iterator.next().getRow))
//        }
      //  fetchMultiRows(Bytes.toString(t.getRow), List[String]("d", "local"))
        hasNextValue = iterator.hasNext
        i+=1
        if(i%100 == 0){
          println(s"scanned $i records")
        }

      } catch {
        case e: Exception =>
          println("Exception encountered during iteration" + e.toString)
          exceptionCounter.inc()
      }
    }

//    val fileReader = new FileReader(s"/Users/ankesh.maheshwari/flipkart/benchmarks/target/xa$threadNumber").getFileReader
//    var line: String = fileReader.readLine()
//    while (line != null) {
//      val dataMap = fetchMultiRows("SM00016_" + line.trim, List[String]("d", "local"))
//      line = fileReader.readLine()
//    }



//    val g = new Get(Bytes.toBytes(rowid))
//    println(g)



  }


  def getHBaseConnHelper = {

    val configServiceHost = ""
    val configServicePort = "80"
    val hConfProps = new Properties()
    /*
        try {
          (hConfProps.setProperty("config.host" , configServiceHost), hConfProps.setProperty("config.port",configServicePort))
        } catch {
          case e: Exception =>
            println("Unable to Connect - " + e.printStackTrace())
        }
    */

    val zookeeperQuorum = ""


    hConfProps.setProperty("hbase.zookeeper.quorum", zookeeperQuorum)
    hConfProps.setProperty("hbase.zookeeper.property.clientPort", "2181")

    val hConfig = ConfigFactory.parseProperties(hConfProps)

//    println("connection : " + hConfig)

    new HTableFactoryWrapper(hConfig, new ConnectionProvider)
  }

  override def beforeAll(): Unit = ???

  override def afterAll(): Unit = ???

  override def runBench(): Unit = ???
}
