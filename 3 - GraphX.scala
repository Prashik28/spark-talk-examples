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

import org.apache.spark.graphx.Edge

val edges = data.sample(true, 0.1, 0) map { line => Edge(line.from.toLong, line.to.toLong, line.amount) }

import org.apache.spark.graphx.Graph

val graph = Graph.fromEdges(edges, ())

graph.cache()

val graph1 = graph.groupEdges(_ + _)

val components = graph1.connectedComponents()

components.vertices.map(_._2).distinct.count

graph1.vertices.count

graph1.outDegrees take 20

val pr = graph1.pageRank(0.1)

pr.vertices take 10

graph1.mapVertices((id, attr) => 1.0).pregel(1d, 5)(
  (_, _, sum) => 0.15 + 0.85 * sum,
  edge => Iterator(edge.dstId -> (edge.srcAttr * edge.attr)),
  _ + _
)