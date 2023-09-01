package com.microsoft.spark.example
import scala.io.StdIn._
object perSquares {
  def main(args: Array[String]) {
    val numCustomers: Int = readInt()
    val billAmount: String = readLine()

    val billAmt: Array[Int] = billAmount.split(" ").map(x => x.toInt)
    var count = 0
    for (i <- billAmt) {
      val sqrt = Math.sqrt(i)
      if (sqrt.ceil - sqrt == 0) {
        count = count + 1
      }
    }
    println(count)
  }
}
