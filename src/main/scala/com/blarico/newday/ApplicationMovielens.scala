package com.blarico.newday


object ApplicationMovielens {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      System.out.println("Please provide path to movielens files, include backslash at the end!")
    }
    if (args.length >= 1) {

    val test: MovielensParseData = new MovielensParseData(args(0))
    test.run()
    }
  }
}
