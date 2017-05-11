import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, lit, desc, sum}
import org.apache.spark.sql.DataFrame

val blacklist = List("viagra", "pharmacy", "buy", 
	"email", "credit", "report", "free", "money",
	"programm", "business", "internet", "work", "financial",
	"mail", "million", "emails", "email", "call", "phone", "card",
	"reports", "marketing", "cost", "opportunity", "income", "cash",
	"easy", "thousands", "mailing", "programs", "simply", "buy", "sell")

val blacklistAuthors = List("Anonymous")
val sqlContext = new SQLContext(sc)
// sc is a variable set by spark-shell
// of type SparkContext

val hypothesis = udf[
	Double, // return
	Double, Double, Double,// x1, x2, x3 (count, blacklist_count and num_of_links)
	Double, Double, Double, Double // C0 .. C3
]((count: Double, blacklist_count: Double, num_of_links: Double, C0: Double, C1: Double, C2: Double, C3: Double) => {
	C0 + (C1 * count) + (C2 * blacklist_count) + (C3 * num_of_links)
})

val hypError = udf[Double, Boolean, Double]((y: Boolean, h: Double) => {
	val output = if(y) 1 else 0
	// Math.pow(output - h, 2) // I need the square for the cost function, but not for the partial detivative of the cost function - cost function derivative
	Math.pow(output - h, 1) // we may not really need the square.
})

val costFunction = udf[Double, DataFrame, Double, Double, Double, Double]((dataset: DataFrame, C0: Double, C1: Double, C2: Double, C3: Double) => {
	// y is the spam. we want to able to grab a function that is close to y as much as possible.
	val sumDf = dataset.agg(sum(hypError($"spam", hypothesis($"count", $"blacklist_count", $"num_of_links", lit(C0), lit(C1), lit(C2), lit(C3)))))
	// sumDf will be a DataFrame of one row and one column.
	// and we are getting the only element in that dataframe.
	(1/(2*dataset.count())) * (sumDf.first.getDouble(0))
})


val data = sqlContext.jsonFile("/Users/elchroy/Roy/Code/bigdata/spiders/results.json").withColumn("spam", lit(false)).drop($"_corrupt_record")
val output = sqlContext.jsonFile("/Users/elchroy/Desktop/2001/output.json").withColumn("spam", lit(true))

val mergedDF = data.unionAll(output)

val countHttp = udf[Int, String]((content:String) => "http".r.findAllIn(content).toList.length)

// normalize function 
val countBlackList = udf[Int, String]((content: String) => (
	blacklist.foldLeft(0) ((acc, key) => {
		acc + key.r.findAllIn(content).toList.length
	})
))

val countBlackListAuthors = udf[Boolean, String]((content: String) => (
	blacklistAuthors.contains(content)
))

val newDF = mergedDF.filter("comment_content is not null").select("author", "comment_content", "spam").withColumn("blacklist_count", countBlackList($"comment_content")).withColumn("is_anonymous", countBlackListAuthors($"author")).withColumn("num_of_links", countHttp($"comment_content")).groupBy($"comment_content", $"author", $"spam", $"is_anonymous", $"num_of_links", $"blacklist_count").agg(count(lit(1)).as("count"))

val sortedDF = newDF.sort(desc("num_of_links"), desc("count"), desc("blacklist_count")).cache() // cache he
// 

val (numOfLinksMin, numOfLinksMax) = sortedDF.agg(min($"num_of_links"), max($"num_of_links")).first match {
  case Row(x: Int, y: Int) => (x, y)
}

val (countMin, countMax) = sortedDF.agg(min($"count"), max($"count")).first match {
  case Row(x: Long, y: Long) => (x, y)
}

val (blacklistCountMin, blacklistCountMax) = sortedDF.agg(min($"blacklist_count"), max($"blacklist_count")).first match {
  case Row(x: Int, y: Int) => (x, y)
}

// val normalize = udf[Double, Int, Int, Int]((value: Int, min: Int, max: Int) => {
// 	(value - min) / (max - min)
// })

val normalizeNumLinks = udf[Double, Int]((value: Int) => {
	(value - numOfLinksMin) / (numOfLinksMax - numOfLinksMin).toDouble
})

val normalizeCount = udf[Double, Int]((value: Int) => {
	(value - countMin) / (countMax - countMin).toDouble
})

val normalizeBlackListCount = udf[Double, Int]((value: Int) => {
	(value - blacklistCountMin) / (blacklistCountMax - blacklistCountMin).toDouble
})

/// this helped us see the actual error becuase the scala was eevaluating in another thread.
println(countMin, countMax)

val normalizedDF = sortedDF.withColumn("blacklist_count", normalizeBlackListCount($"blacklist_count")).withColumn("num_of_links", normalizeNumLinks($"num_of_links")).withColumn("count", normalizeCount($"count"))

def sigma(numLinks: Double, count: Double, blacklistCount: Double) : Double = {
	// return a number between 0 and 1.
	// we're using the average of the numbers.

	// (numLinks + count + blacklistCount) / 3
	(numLinks + count + blacklistCount * 2) / 4
}

val spamFactor = udf[Double, Double, Double, Double](sigma)
val finalDF = normalizedDF.withColumn("spam_factor", spamFactor($"num_of_links", $"count", $"blacklist_count")).sort(desc("spam_factor"))
val limit = finalDF.stat.approxQuantile("spam_factor", Array(0.98), 0).last
val realFinalUDF = udf[Boolean, Double]((spam_factor: Double) => (spam_factor >= limit))
val realFinalDF = finalDF.withColumn("spam", realFinalUDF($"spam_factor")).cache()

realFinalDF.show(20)



def costFunctionDerivative(dataset: DataFrame, C0: Double, C1: Double, C2: Double, C3: Double, j: Int) : Double = {
	val x = j match {
		case 0 => lit(1)
		case 1 => dataset("count")
		case 2 => dataset("blacklist_count")
		case 3 => dataset("num_of_links")
	}
	val sumDf = dataset.agg(sum(x * hypError($"spam", hypothesis($"count", $"blacklist_count", $"num_of_links", lit(C0), lit(C1), lit(C2), lit(C3)))))
	// Cj := Cj - a * 1.0/m * sum(1, m, i, (h(xi) - yi) * x{i, j})
	(1.0/dataset.count()) * (sumDf.first.getDouble(0)) // take not of integer division. Int / Int = Int, 
}

def gradientDescent (dataset: DataFrame) : (Double, Double, Double, Double) = {
	val alpha = 0.03 // this is the learning rate for the gradientDescent funciton.
	// to avoid overfitting, you can use a smaller value.
	// but his can make the grddient descent to be very slow.

	var C0 = 0.0
	var C1 = 0.0
	var C2 = 0.0
	var C3 = 0.0

	var prevC0 = -1.0
	var prevC1 = -1.0
	var prevC2 = -1.0
	var prevC3 = -1.0

	var temp0 = 0.0
	var temp1 = 0.0
	var temp2 = 0.0
	var temp3 = 0.0

	var count = 0;
	var start = System.currentTimeMillis

	while (C0 != prevC0 && C1 != prevC1 && C2 != prevC2 && C3 != prevC3) {
		// store the paramters inside temporary values. This is very important in gradient descent.
		temp0 = C0 - (alpha * costFunctionDerivative(dataset, C0, C1, C2, C3, 0))
		temp1 = C1 - (alpha * costFunctionDerivative(dataset, C0, C1, C2, C3, 1))
		temp2 = C2 - (alpha * costFunctionDerivative(dataset, C0, C1, C2, C3, 2))
		temp3 = C3 - (alpha * costFunctionDerivative(dataset, C0, C1, C2, C3, 3))
		// println("=========>")
		// println(temp0, temp1, temp2, temp3, "debugging the temp variables")

		// update the previous parameters with the values of C0...C3
		prevC0 = C0
		prevC1 = C1
		prevC2 = C2
		prevC3 = C3
		// println("=========>")
		// println(prevC0, prevC1, prevC2, prevC3, "debugging the prev variables")

		// assign the new updated values to the c parameters.
		C0 = temp0
		C1 = temp1
		C2 = temp2
		C3 = temp3

		// println("=========>")
		println(C0, C1, C2, C3, count, "<========debugging the return values")
		// println("Time spent ========> ", System.currentTimeMillis - start, "\n")
		count += 1
	}
	var stop = System.currentTimeMillis
	println("Total time =====> ", stop - start)
	(C0, C1, C2, C3) // return a tuple with 4 Doubles
}

// val spamFactorLimit = finalDF.agg(min($"blacklist_count"), max($"blacklist_count")).first match {
//   case Row(x: Int, y: Int) => (x, y)
// }



