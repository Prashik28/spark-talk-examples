// SPARK_JAVA_OPTS="-Dspark.cleaner.ttl=86400" bin/spark-shell

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

val ssc = new StreamingContext(sc, Seconds(5))

val stream = ssc.textFileStream("/home/papillon/prova/input")
// Also sockets, Kafka, ...

case class Line(
  from: String,
  to: String,
  amount: Double,
  currency: String,
  date: Long
)

val data = stream map { line =>
  val Array(from, to, amount, currency, date) = line.split('\t')

  Line(from, to, amount.toDouble, currency, date.toLong)
}

val eur = data filter (_.currency == "EUR")

val involved = eur map (_.from)

involved.countByValueAndWindow(Seconds(20), Seconds(10)).
  map({ case (v, count) => s"$v\t$count" }).
  saveAsTextFiles("/home/papillon/prova/output/result")

val eurUpdates = eur flatMap { line =>
  Iterator(
    (line.from -> -line.amount),
    (line.to -> line.amount)
  )
}

val eurBalance = eurUpdates updateStateByKey { (updates: Seq[Double], total: Option[Double]) =>
  val start = total getOrElse 0d
  val newTotal = updates.foldLeft(start)(_ + _)

  Some(newTotal)
}

eurBalance.
  map({ case (key, total) => s"$key\t$total" }).
  saveAsTextFiles("/home/papillon/prova/balance/result")

ssc.checkpoint("/home/papillon/prova/checkpoint")

ssc.start()
