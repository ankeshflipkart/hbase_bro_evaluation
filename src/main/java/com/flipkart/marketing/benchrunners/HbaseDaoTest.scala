package com.flipkart.marketing.benchrunner

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import com.flipkart.marketing.benchrunners.{ThreadIngestor, ThreadWorker}
import com.flipkart.marketing.commons.behaviours.Bench
import com.flipkart.marketing.commons.connections.ConnectionProvider
import com.flipkart.marketing.commons.factories.HTableFactoryWrapper
import com.flipkart.marketing.dao.HbaseDao
import com.typesafe.config.ConfigFactory
import com.yammer.metrics.core.{MetricsRegistry, TimerContext}
import com.yammer.metrics.reporting.ConsoleReporter
import org.apache.hadoop.hbase.client.{ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 *
 * @author durga.s
 * @version 11/18/15
 */
class HbaseDaoTest extends HbaseDao with Bench with Runnable{

  var hConnectionHelper = getHBaseConnHelper
  val tblName = "cross_pivot_instances"
  var registry: MetricsRegistry = null
  val maxThreadCount = 50
  val executor = Executors.newFixedThreadPool(maxThreadCount)


  override def runBench() = {
    implicit var h = hConnectionHelper.getTableInterface(tblName)
    var threadNumber = 1
    val registry = new MetricsRegistry()
    val exceptionCounter = registry.newCounter(this.getClass, "HBase-Exception-Counter")
    ConsoleReporter.enable(registry, 20, TimeUnit.SECONDS)
    while(threadNumber < maxThreadCount) {
      executor.submit(new ThreadWorker(hConnectionHelper, threadNumber, exceptionCounter, registry))
      println(s"Spawned thread: $threadNumber")
      threadNumber = threadNumber + 1
    }
  }

  def runIngestor() = {
    implicit var h = hConnectionHelper.getTableInterface(tblName)
    var threadNumber = 1

    val registry = new MetricsRegistry()
    val timer = registry.newTimer(this.getClass, "HBase-Ingestion-Timer")

    val exceptionCounter = registry.newCounter(this.getClass, "HBase-Exception-Counter")
    ConsoleReporter.enable(registry, 20, TimeUnit.SECONDS)

    while(threadNumber < 500) {
      executor.submit(new ThreadIngestor(hConnectionHelper, threadNumber, timer, exceptionCounter, registry))
      threadNumber = threadNumber + 1
    }
  }

  def rangeScan(_registry: MetricsRegistry) = {
    registry = _registry
    val timerR = registry.newTimer(this.getClass, "HBase-Range-Timer")
    val timerIter = registry.newTimer(this.getClass, "HBase-IteratorGet-Timer")
    val timerScan = registry.newTimer(this.getClass, "HBase-Scan-Timer")
//    val cnt : Counter = registry.newCounter(this.getClass, "HBase-Range-Timer")
//    val exceptionCounter = registry.newCounter(this.getClass, "HBase-Exception-Counter")
    implicit var h = hConnectionHelper.getTableInterface(tblName)
//    val prefix = Bytes.toBytes("PDN_PID")
    ConsoleReporter.enable(registry, 1, TimeUnit.SECONDS)

    var ctx: TimerContext = null
    ctx = timerR.time()

    var ctxScan : TimerContext = null
    ctxScan = timerScan.time()
    val scan: Scan = new Scan(Bytes.toBytes("TSRQPLCDEFG_HIJKMNOP"),Bytes.toBytes("TSRQPLCDEFG_HIJKMNOP~"))
    val rs: ResultScanner = h.getScanner(scan)
    ctxScan.stop()
    val iter = rs.iterator()


    var hasNextValue = iter.hasNext
    var count = 0
    while (hasNextValue) {
      var ctxIter : TimerContext = null
      ctxIter = timerIter.time()
      iter.next()
      hasNextValue = iter.hasNext
      ctxIter.stop()
      count += 1
    }
    println("Completed scan.")
    ctx.stop()

    Thread.sleep(5000)


  }

  override def afterAll() = {
    println("triggering cleanup afterAll")
    hConnectionHelper.shutdown()
    println("cleanup successful")
  }


  def getHBaseConnHelper = {

    val configServiceHost = "10.47.0.101"
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

    val zookeeperQuorum = "10.33.17.204,10.33.209.206,10.33.193.227"


    hConfProps.setProperty("hbase.zookeeper.quorum", zookeeperQuorum)
    hConfProps.setProperty("hbase.zookeeper.property.clientPort", "2181")

    val hConfig = ConfigFactory.parseProperties(hConfProps)

//    println("connection : " + hConfig)

    new HTableFactoryWrapper(hConfig, new ConnectionProvider)
  }

  def bench1 = {}
  def bench2 = {}

  //  "A row put operation for single columnFamily" should "throw no IOException" in {
  //
  //    implicit var h = hConnectionHelper.getTableInterface(tblName)
  //    try {
  //      val rowKey = UUID.randomUUID().toString
  //      val data = Map[String, Array[Byte]](
  //          "deviceId" -> "0b6dc5db9fd9f664438f4f9ea03e53d7".getBytes(CharEncoding.UTF_8),
  //          "make" -> "Motorola".getBytes(CharEncoding.UTF_8),
  //          "osVersion" -> "4.4.4".getBytes(CharEncoding.UTF_8),
  //          "app" -> "Retail".getBytes(CharEncoding.UTF_8),
  //          "platform" -> "Android".getBytes(CharEncoding.UTF_8),
  //          "deviceId" -> "0b6dc5db9fd9f664438f4f9ea03e53d7".getBytes(CharEncoding.UTF_8)
  //        )
  //
  //      addRow(rowKey, Map[String, Map[String, Array[Byte]]]("p" -> data))
  //
  //      println("inserted hbase table row: %s".format(data.toString()))
  //    } finally {
  //      hConnectionHelper.releaseTableInterface(h)
  //    }
  //  }
  //
  //
  //  var rowKey = UUID.randomUUID().toString
  //  "A row put operation for multiple columnFamilies" should "throw no IOException" in {
  //    implicit val h = hConnectionHelper.getTableInterface(tblName)
  //
  //    try {
  //      val dataPrimary = Map[String, Array[Byte]](
  //        "deviceId" -> "0b6dc5db9fd9f664438f4f9ea03e53d7".getBytes(CharEncoding.UTF_8),
  //        "make" -> "Motorola".getBytes(CharEncoding.UTF_8),
  //        "osVersion" -> "4.4.4".getBytes(CharEncoding.UTF_8),
  //        "app" -> "Retail".getBytes(CharEncoding.UTF_8),
  //        "platform" -> "Android".getBytes(CharEncoding.UTF_8),
  //        "deviceId" -> "0b6dc5db9fd9f664438f4f9ea03e53d7".getBytes(CharEncoding.UTF_8)
  //      )
  //
  //      val dataAuxiliary = Map[String, Array[Byte]](
  //        ("optIn", "true".getBytes(CharEncoding.UTF_8)),
  //        ("company", "fkint".getBytes(CharEncoding.UTF_8)),
  //        ("org", "mp".getBytes(CharEncoding.UTF_8)),
  //        ("namespace", "ceryx".getBytes(CharEncoding.UTF_8))
  //      )
  //
  //      addRow(rowKey, Map[String, Map[String, Array[Byte]]]("p" -> dataPrimary, "a" -> dataAuxiliary))
  //
  //      println("inserted hbase table row:\np: %s \na: %s ".format(dataPrimary.toString(), dataAuxiliary.toString()))
  //    } finally {
  //      hConnectionHelper.releaseTableInterface(h)
  //    }
  //  }
  //
  //  "Get operation for a row" should "throw no IOException" in {
  //    implicit val h = hConnectionHelper.getTableInterface(tblName)
  //
  //    try {
  //      val result = fetchRow(rowKey, List[String]("p", "a"))
  //      println("result for [%s] is :\n%s".format(rowKey, result.toString()))
  //    } finally {
  //      hConnectionHelper.releaseTableInterface(h)
  //    }
  //  }
  //
  //  "Get multi operation for a row" should "throw no IOException" in {
  //    implicit val h = hConnectionHelper.getTableInterface(tblName)
  //
  //    try {
  //      val result = fetchMultiRows(List(rowKey), List[String]("p", "a"))
  //      println("result for [%s] is :\n%s".format(rowKey, result(rowKey).toString()))
  //    } finally {
  //      hConnectionHelper.releaseTableInterface(h)
  //    }
  //  }
  override def beforeAll(): Unit = ???

  override def run(): Unit = ???
}
