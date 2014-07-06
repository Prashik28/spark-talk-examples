val rdd = sc.textFile("/home/papillon/fake-transactions.tsv")

case class Line(
  payer: String,
  payee: String,
  amount: Double,
  currency: String,
  date: Long
)

val payments = rdd map { line =>
  val Array(payer, payee, amount, currency, date) = line.split('\t')

  Line(payer, payee, amount.toDouble, currency, date.toLong)
}

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import sqlContext._

payments.registerAsTable("payments")

val payees = sql("SELECT payee FROM payments WHERE payer = '000000345'")

payees.collect

val payees1 = payments.where('payer == "000000345").select('payee)

payees1.collect

payments.saveAsParquetFile("/home/papillon/payments.parquet")

///////////////

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val payments = sqlContext.parquetFile("/home/papillon/payments.parquet")

payments.registerAsTable("payments")

import sqlContext._

payments.registerAsTable("payments")

val payees = sql("SELECT payee FROM payments WHERE payer = '000000345'")

payees.collect