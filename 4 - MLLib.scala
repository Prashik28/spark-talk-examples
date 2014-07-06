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

def features(lines: Seq[Line]) =
  Array(
    lines.map(_.amount).sum / lines.length,
    (lines.map(_.date).max - lines.map(_.date).min).toDouble,
    lines.map(_.to).distinct.length.toDouble
  )

val feat = data groupBy (_.from) mapValues { lines =>
  Array(
    lines.map(_.amount).sum / lines.length,
    (lines.map(_.date).max - lines.map(_.date).min).toDouble,
    lines.map(_.to).distinct.length.toDouble
  )
}

val rawFeatures = feat map (_._2)

import org.apache.spark.mllib.clustering.KMeans

val model = KMeans.train(rawFeatures, k = 5, maxIterations = 10)

model.clusterCenters

model.computeCost(rawFeatures)