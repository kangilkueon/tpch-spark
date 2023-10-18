package tpch

import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession

// TPC-H table schemas
case class Customer(
  c_custkey: Long,    // primary key
  c_name: String,
  c_address: String,
  c_nationkey: Long,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Long,   // primary key
  l_partkey: Long,
  l_suppkey: Long,
  l_linenumber: Long, // primary key
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Long,    // primary key
  n_name: String,
  n_regionkey: Long,
  n_comment: String)

case class Order(
  o_orderkey: Long,   // primary key
  o_custkey: Long,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Long,
  o_comment: String)

case class Part(
  p_partkey: Long,    // primary key
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Long,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Long,   // primary key
  ps_suppkey: Long,   // primary key
  ps_availqty: Long,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Long,    // primary key
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Long,    // primary key
  s_name: String,
  s_address: String,
  s_nationkey: Long,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

class TpchSchemaProvider(spark: SparkSession, inputDir: String, csdDir: String, useCSDOffload: Boolean) {
  import spark.implicits._


  val dfMap = if (useCSDOffload == true) {
    println("== CSD Offload ==");
    Map(
      "customer" -> spark.read.textFile(inputDir + "/customer.tbl*").map(_.split('|')).map(p =>
        Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

      // "lineitem" -> spark.read.textFile(inputDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
      "lineitem" -> spark.read.textFile(csdDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
        Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

      "nation" -> spark.read.textFile(inputDir + "/nation.tbl*").map(_.split('|')).map(p =>
        Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF(),

      "region" -> spark.read.textFile(inputDir + "/region.tbl*").map(_.split('|')).map(p =>
        Region(p(0).trim.toLong, p(1).trim, p(2).trim)).toDF(),

      "order" -> spark.read.textFile(inputDir + "/orders.tbl*").map(_.split('|')).map(p =>
        Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF(),

      "part" -> spark.read.textFile(inputDir + "/part.tbl*").map(_.split('|')).map(p =>
        Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

      "partsupp" -> spark.read.textFile(inputDir + "/partsupp.tbl*").map(_.split('|')).map(p =>
        Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF(),

      "supplier" -> spark.read.textFile(inputDir + "/supplier.tbl*").map(_.split('|')).map(p =>
        Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
    )
  } else {
    Map(
      "customer" -> spark.read.textFile(inputDir + "/customer.tbl*").map(_.split('|')).map(p =>
        Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

      "lineitem" -> spark.read.textFile(inputDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
        Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

      "nation" -> spark.read.textFile(inputDir + "/nation.tbl*").map(_.split('|')).map(p =>
        Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF(),

      "region" -> spark.read.textFile(inputDir + "/region.tbl*").map(_.split('|')).map(p =>
        Region(p(0).trim.toLong, p(1).trim, p(2).trim)).toDF(),

      "order" -> spark.read.textFile(inputDir + "/orders.tbl*").map(_.split('|')).map(p =>
        Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF(),

      "part" -> spark.read.textFile(inputDir + "/part.tbl*").map(_.split('|')).map(p =>
        Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

      "partsupp" -> spark.read.textFile(inputDir + "/partsupp.tbl*").map(_.split('|')).map(p =>
        Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF(),

      "supplier" -> spark.read.textFile(inputDir + "/supplier.tbl*").map(_.split('|')).map(p =>
        Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
    )
  } 

  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("order").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }

  def getSchema(tableName: String): StructType =
    tableName match {
      case "lineitem" => Encoders.product[Lineitem].schema
      case "nation" => Encoders.product[Nation].schema
      case "order" => Encoders.product[Order].schema
      case "orders" => Encoders.product[Order].schema
      case "region" => Encoders.product[Region].schema
      case "part" => Encoders.product[Part].schema
      case "customer" => Encoders.product[Customer].schema
      case "supplier" => Encoders.product[Supplier].schema
      case "partsupp" => Encoders.product[Partsupp].schema
    }
}



object TpchSchemaProviderTemp {
  val lineitem = StructType(
    StructField("l_orderkey", LongType, false) ::
      StructField("l_partkey", LongType, false) ::
      StructField("l_suppkey", LongType, false) ::
      StructField("l_linenumber", LongType, false) ::
      StructField("l_quantity", DoubleType, false) ::
      StructField("l_extendedprice", DoubleType, false) ::
      StructField("l_discount", DoubleType, false) ::
      StructField("l_tax", DoubleType, false) ::
      StructField("l_returnflag", StringType, false) ::
      StructField("l_linestatus", StringType, false) ::
      StructField("l_shipdate", StringType, false) ::
      StructField("l_commitdate", StringType, false) ::
      StructField("l_receiptdate", StringType, false) ::
      StructField("l_shipinstruct", StringType, false) ::
      StructField("l_shipmode", StringType, false) ::
      StructField("l_comment", StringType, false) :: Nil)
  val nation = StructType(
    StructField("n_nationkey", LongType, false) ::
      StructField("n_name", StringType, false) ::
      StructField("n_regionkey", LongType, false) ::
      StructField("n_comment", StringType, false) :: Nil)
  val order = StructType(
    StructField("o_orderkey", LongType, false) ::
      StructField("o_custkey", LongType, false) ::
      StructField("o_orderstatus", StringType, false) ::
      StructField("o_totalprice", DoubleType, false) ::
      StructField("o_orderdate", StringType, false) ::
      StructField("o_orderpriority", StringType, false) ::
      StructField("o_clerk", StringType, false) ::
      StructField("o_shippriority", LongType, false) ::
      StructField("o_comment", StringType, false) :: Nil)
  val region = StructType(
    StructField("r_regionkey", LongType, false) ::
      StructField("r_name", StringType, false) ::
      StructField("r_comment", StringType, false) :: Nil)
  val part = StructType(
    StructField("p_partkey", LongType, false) ::
      StructField("p_name", StringType, false) ::
      StructField("p_mfgr", StringType, false) ::
      StructField("p_brand", StringType, false) ::
      StructField("p_type", StringType, false) ::
      StructField("p_size", LongType, false) ::
      StructField("p_container", StringType, false) ::
      StructField("p_retailprice", DoubleType, false) ::
      StructField("p_comment", StringType, false) :: Nil)
  val customer = StructType(
    StructField("c_custkey", LongType, false) ::
      StructField("c_name", StringType, false) ::
      StructField("c_address", StringType, false) ::
      StructField("c_nationkey", LongType, false) ::
      StructField("c_phone", StringType, false) ::
      StructField("c_acctbal", DoubleType, false) ::
      StructField("c_mktsegment", StringType, false) ::
      StructField("c_comment", StringType, false) :: Nil)
  val supplier = StructType(
    StructField("s_suppkey", LongType, false) ::
      StructField("s_name", StringType, false) ::
      StructField("s_address", StringType, false) ::
      StructField("s_nationkey", LongType, false) ::
      StructField("s_phone", StringType, false) ::
      StructField("s_acctbal", DoubleType, false) ::
      StructField("s_comment", StringType, false) :: Nil)
  val partsupp = StructType(
    StructField("ps_partkey", LongType, false) ::
      StructField("ps_suppkey", LongType, false) ::
      StructField("ps_availqty", LongType, false) ::
      StructField("ps_supplycost", DoubleType, false) ::
      StructField("ps_comment", StringType, false) :: Nil)
  def getSchema(tableName: String): StructType =
    tableName match {
      case "lineitem" => lineitem
      case "nation" => nation
      case "order" => order
      case "orders" => order
      case "region" => region
      case "part" => part
      case "customer" => customer
      case "supplier" => supplier
      case "partsupp" => partsupp
    }
}
