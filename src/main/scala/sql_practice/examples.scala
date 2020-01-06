package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }
  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demo = spark.read.json("data/input/demographie_par_commune.json")
    val dep = spark.read.csv("data/input/departements.txt").select($"_c0" as "name",$"_c1" as "id")

    demo.select(sum($"population")).show

    val depWithDemo = demo.select($"departement",$"population").join(dep,$"departement"===$"id").select($"name" as "departement",$"id" as "code", $"population").groupBy($"departement",$"code").agg(sum(($"population")) as "population")

    depWithDemo.select($"code",$"population").orderBy($"population".desc).show

    depWithDemo.select($"departement",$"population").orderBy($"population".desc).show

  }
}
