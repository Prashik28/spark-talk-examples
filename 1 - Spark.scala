val rdd = sc.textFile("/home/papillon/fake-transactions.tsv")

case class Line(
  from: String,
  to: String,
  amount: Double,
  currency: String,
  date: Long
)

val data = rdd map { line =>
  val Array(from, to, amount, currency, date) = line.split('\t')

  Line(from, to, amount.toDouble, currency, date.toLong)
}

val currencies = data.map(_.currency).distinct

currencies.collect

val amounts = data map (_.amount)

val amountSum = amounts reduce (_ + _)

val avgAmount = amountSum / amounts.count

val filtered = data filter (_.amount > 70000)

val outflow = data groupBy (_.from) mapValues (_.map(_.amount).sum)

outflow filter { case (k, v) => v > 1000000 } count

outflow filter { case (k, v) => v > 1000000 } collect