package com.flipkart.marketing.benchrunners

import java.util.Properties

import com.flipkart.marketing.commons.behaviours.Bench
import com.flipkart.marketing.commons.connections.ConnectionProvider
import com.flipkart.marketing.commons.factories.HTableFactoryWrapper
import com.flipkart.marketing.dao.HbaseDao
import com.typesafe.config.ConfigFactory
import com.yammer.metrics.core.{Counter, MetricsRegistry, Timer, TimerContext}
import org.apache.hadoop.hbase.client.{ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes

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
    this.maxThreadCount = maxThreadCount;
  }
  val tblName = "cross_pivot_instance_test"

  override def run(): Unit = {
    val fullScanTimer = registry.newTimer(this.getClass, "HBase-Full-Scan-Timer")
    val getNextTimer = registry.newTimer(this.getClass, "HBase-Get-Next-Timer")
    val queryTimer = registry.newTimer(this.getClass, "HBase-Query-Timer")
    implicit var h = hConnectionHelper.getTableInterface(tblName)

    for (index <- 0 to rowKeys.size)
    {
      for (i <- 1 to maxThreadCount) {
        val starRowKey = rowKeys(index).replace("N", i.toString)
        val endRowKey = starRowKey + "~"
        val scan: Scan = new Scan(Bytes.toBytes(starRowKey), Bytes.toBytes(endRowKey))
        //We've to set the prefetch count, and make sure this doesn't populate the blockCache
        scan.setCaching(5000)
        scan.setCacheBlocks(false)

        val ctxRangeQuery = queryTimer.time()
        val rs: ResultScanner = h.getScanner(scan)
        ctxRangeQuery.stop()

        val iterator = rs.iterator()
        var hasNextValue = iterator.hasNext
        var count = 0

        val ctxFullScan  = fullScanTimer.time()

        while (hasNextValue) {
          val ctxGetNext = getNextTimer.time()
          try {
            iterator.next()
            hasNextValue = iterator.hasNext
            ctxGetNext.stop()
          } catch {
            case e: Exception =>
              ctxGetNext.stop()
              println("Exception encountered during iteration" + e.toString)
              exceptionCounter.inc()
          }
          count += 1
        }
        ctxFullScan.stop()
        println(s"Thread ${Thread.currentThread().getName}: Completed RangeScan for $starRowKey")
      }
    }
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

  override def beforeAll(): Unit = ???

  override def afterAll(): Unit = ???

  override def runBench(): Unit = ???
}
