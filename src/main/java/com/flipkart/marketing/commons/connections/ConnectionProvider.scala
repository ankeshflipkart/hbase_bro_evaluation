package com.flipkart.marketing.commons.connections

import java.util.Properties
import javax.sql.DataSource

import org.apache.commons.dbcp2.BasicDataSourceFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager}

import scala.collection.JavaConverters._

/**
  * Created by kinshuk.bairagi on 27/01/16.
  */

class ConnectionProvider extends TConnectionProvider {

  override def createHbaseConnection(hConnConfig: Configuration): HConnection = HConnectionManager.createConnection(hConnConfig)

  override def createDatasourceConnection(mySQLProperties: Properties): DataSource = BasicDataSourceFactory.createDataSource(mySQLProperties)

}