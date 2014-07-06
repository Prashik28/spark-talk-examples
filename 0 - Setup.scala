def pad(s: String, n: Int) =
  List.fill(n - s.length)('0').mkString + s

import scala.util.Random._

def nextId = pad(nextInt(100000).toString, 9)

sealed trait Currency
case object EUR extends Currency
case object HUF extends Currency
case object USD extends Currency

def nextCurrency: Currency = nextInt(3) match {
  case 0 => EUR
  case 1 => HUF
  case _ => USD
}

case class Line(
  from: String,
  to: String,
  amount: Double,
  currency: Currency,
  date: Long
)

def nextLine = Line(nextId, nextId, 100000 * nextDouble, nextCurrency, 1404120000000L + nextInt)

val seq = List.fill(1000000)(nextLine)

def print(line: Line) =
  line.productIterator.toList map (_.toString) mkString "\t"

def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

import java.io._

printToFile(new File("/home/papillon/fake-transactions-low.tsv"))(p => {
  seq foreach { s => p.println(print(s)) }
})