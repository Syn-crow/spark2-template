package sql_practice

import org.apache.hadoop.fs.shell.Count
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
  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s07 = spark.read.option("sep", "\t").csv("data/input/sample_07").select($"_c0" as "code", $"_c1" as "description", $"_c2" as "total_emp", $"_c3" as "salary")

    //s07.show

    val s08 = spark.read.option("sep", "\t").csv("data/input/sample_08").select($"_c0" as "code", $"_c1" as "description", $"_c2" as "total_emp", $"_c3" as "salary")

    //s08.show

    s07.filter($"salary">100000).orderBy($"salary".desc).show

    val joined = s07.join(s08, s07("code") === s08("code"))

    joined.select(s07("code"),s07("description"),s08("salary")-s07("salary") as "salaryGrowth").orderBy($"salaryGrowth".desc).show

    joined.filter(s07("salary")>100000).select(s07("code"),s07("description"),s07("total_emp")-s08("total_emp") as "jobLoss").filter($"jobLoss">0).orderBy($"jobLoss".desc).show
  }
  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    toursDF.show(300)

    println(toursDF.select($"tourDifficulty").distinct().count())

    toursDF.select(min($"tourPrice") as "minPrice",avg($"tourPrice") as "avgPrice", max($"tourPrice") as "maxPrice").show

    toursDF.groupBy($"tourDifficulty").agg(min($"tourPrice") as "minPrice",avg($"tourPrice") as "avgPrice", max($"tourPrice") as "maxPrice").show

    toursDF.withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").count().orderBy($"count".desc).show(10)

    val easyCount = toursDF.filter($"tourDifficulty" === "Easy").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").count().select($"tourTags" as "t1",$"count" as "easyCount")
    val mediumCount = toursDF.filter($"tourDifficulty" === "Medium").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").count().select($"tourTags" as "t2",$"count" as "mediumCount")
    val difficultCount = toursDF.filter($"tourDifficulty" === "Difficult").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").count().select($"tourTags" as "t3",$"count" as "difficultCount")
    val variesCount = toursDF.filter($"tourDifficulty" === "Varies").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").count().select($"tourTags" as "t4",$"count" as "variesCount")

    toursDF.withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").count().join(easyCount, $"tourTags"===$"t1","left").join(mediumCount, $"tourTags"===$"t2","left").join(difficultCount, $"tourTags"===$"t3","left").join(variesCount, $"tourTags"===$"t4","left").select($"tourTags",$"count" as "total",$"easyCount",$"mediumCount",$"difficultCount",$"variesCount").orderBy($"total".desc).show(10)

    val easyPrice = toursDF.filter($"tourDifficulty" === "Easy").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").agg(min($"tourPrice") as "easyMinPrice",avg($"tourPrice") as "easyAvgPrice", max($"tourPrice") as "easyMaxPrice").select($"tourTags" as "t1",$"easyMinPrice",$"easyAvgPrice",$"easyMaxPrice")
    val mediumPrice = toursDF.filter($"tourDifficulty" === "Medium").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").agg(min($"tourPrice") as "mediumMinPrice",avg($"tourPrice") as "mediumAvgPrice", max($"tourPrice") as "mediumMaxPrice").select($"tourTags" as "t2",$"mediumMinPrice",$"mediumAvgPrice",$"mediumMaxPrice")
    val difficultPrice = toursDF.filter($"tourDifficulty" === "Difficult").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").agg(min($"tourPrice") as "difficultMinPrice",avg($"tourPrice") as "difficultAvgPrice", max($"tourPrice") as "difficultMaxPrice").select($"tourTags" as "t3",$"difficultMinPrice",$"difficultAvgPrice",$"difficultMaxPrice")
    val variesPrice = toursDF.filter($"tourDifficulty" === "Varies").withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").agg(min($"tourPrice") as "variesMinPrice",avg($"tourPrice") as "variesAvgPrice", max($"tourPrice") as "variesMaxPrice").select($"tourTags" as "t4",$"variesMinPrice",$"variesAvgPrice",$"variesMaxPrice")

    toursDF.withColumn("tourTags", explode($"tourTags")).groupBy($"tourTags").count().join(easyPrice, $"tourTags"===$"t1","left").join(mediumPrice, $"tourTags"===$"t2","left").join(difficultPrice, $"tourTags"===$"t3","left").join(variesPrice, $"tourTags"===$"t4","left").select($"tourTags",$"count",$"easyMinPrice",$"easyAvgPrice",$"easyMaxPrice",$"mediumMinPrice",$"mediumAvgPrice",$"mediumMaxPrice",$"difficultMinPrice",$"difficultAvgPrice",$"difficultMaxPrice",$"variesMinPrice",$"variesAvgPrice",$"variesMaxPrice").orderBy($"count".desc).show(10)

  }
}
