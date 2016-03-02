package com.flipkart.marketing.commons.factories

import com.flipkart.marketing.commons.behaviours.HTableFactory
import com.flipkart.marketing.commons.connections.TConnectionProvider
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTableInterface, HConnection}
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}

/**
 *
 *
 * @author durga.s
 * @version 11/16/15
 */
class HTableFactoryWrapper(hConnConfig: Config, connProvider: TConnectionProvider) extends HTableFactory {

  val hConnectionConfig = {
    val hConfig: Configuration = HBaseConfiguration.create()
    hConfig.set(HConstants.ZOOKEEPER_QUORUM, hConnConfig.getString(HConstants.ZOOKEEPER_QUORUM))
    hConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, hConnConfig.getString(HConstants.ZOOKEEPER_CLIENT_PORT))
    hConfig.set(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD.toString)
    hConfig.set(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT.toString)
    hConfig.set("hbase.zookeeper.watcher.sync.connected.wait", "5000")
//    println(s"HConnectionConfig: ${hConfig.toString}")
    hConfig
  }

  var hConnection: HConnection = connProvider.createHbaseConnection(hConnectionConfig)

  override def shutdown(): Unit = hConnection.close()

  override def getTableInterface(tableName: String): HTableInterface = hConnection.getTable(tableName)

  override def releaseTableInterface(hTableInterface: HTableInterface): Unit = hTableInterface.close()
}
