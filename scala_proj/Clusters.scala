import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.math.log

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, lit, desc, sum, unix_timestamp, lower}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StringIndexer



import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder, RegexTokenizer, StopWordsRemover, NGram}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.MulticlassMetrics



import java.net.URL
// val t = "https://www.bell.com?token=123"
// val url = new URL(t).getHost 

val sqlContext = new SQLContext(sc)

val dateToTS = unix_timestamp($"date_created", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
val authorToLower = udf[String, String]((author: String) => { author.toLowerCase() })
// yyyy-MM-dd'T'HH:mm:ss.SSSXXX

val store = collection.mutable.Map.empty[String, Int]

val getAuthorIndex = udf[Int, String]( (author: String) => {
	store.getOrElse(author, {
		val index = store.size
		// store.set(author, index)
		store += (author -> index) // put the new author into the array of authors
		index
	})
})

val getPostURLHost = udf[String, String]((post_url: String) => {
	new URL(post_url).getHost 
})

val authorIndexer = (new StringIndexer()
  .setInputCol("author")
  .setOutputCol("authorIndex"))

val postURLIndexer = (new StringIndexer()
  .setInputCol("post_url")
  .setOutputCol("post_url_index"))

val post_content_tokenizer = (new RegexTokenizer()
	.setInputCol("post_content")
	.setOutputCol("post_content_words")
	.setPattern("\\W"))

val post_content_stop_words_remover = (new StopWordsRemover()
	.setInputCol("post_content_words")
	.setOutputCol("post_content_keywords"))

val post_content_ngrammer = (new NGram()
	.setInputCol("post_content_keywords")
	.setOutputCol("post_content_ngrams")
	.setN(10)) // divides the sequence of tokens in a new one, that wo
// the number of tokens that our tranformation can make.

val title_tokenizer = (new RegexTokenizer()
	.setInputCol("title")
	.setOutputCol("title_words")
	.setPattern("\\W"))

val title_stop_words_remover = (new StopWordsRemover()
	.setInputCol("title_words")
	.setOutputCol("title_keywords"))

val title_ngrammer = (new NGram()
	.setInputCol("title_keywords")
	.setOutputCol("title_ngrams")
	.setN(2))

val data  = (sqlContext.jsonFile("/Users/elchroy/Roy/Code/bigdata/spiders/data.json")
	.filter("author is not null")
	.drop($"_corrupt_record")
	.withColumn("ts", dateToTS)
	.withColumn("author", lower($"author"))
	.withColumn("my_author_index", getAuthorIndex($"author"))
)

val pipeline = new Pipeline().setStages(Array(authorIndexer, postURLIndexer, post_content_tokenizer, post_content_stop_words_remover, post_content_ngrammer, title_tokenizer, title_stop_words_remover, title_ngrammer))
val indexed = pipeline.fit(data).transform(data)

// indexed.show()

val post_content_corpus = (indexed
	.select("post_content_keywords")
	.collect()
	.map{row => row.getAs[Seq[String]](0)}
)
// println(post_content_corpus)

// var unique_post_content_set = Set.empty[String]
var unique_post_content_set = Map.empty[String, Double]

post_content_corpus.foreach((document) => {
	// document.toSeq.foreach(word => unique_post_content_set += word.toString())
	document.toSeq.map(word => unique_post_content_set += (word -> 0))
})

unique_post_content_set.toSeq.foreach((kv_pair) => {
	val word = kv_pair._1
	val df = post_content_corpus.filter(_.contains(word)).length
	// unique_post_content_set = unique_post_content_set.set(word, log((post_content_corpus.length) / (df + 1)))
	unique_post_content_set += (word -> log((post_content_corpus.length) / (df + 1))) // plus 1 => if df is 0, we'll get an error.
})

val post_content_tfidfVector = udf[Seq[Double], Seq[String]]((document: Seq[String]) => {
	unique_post_content_set.map{ kv_pair => 
		val word = kv_pair._1
		if (document.contains(word)) {
			val idf = unique_post_content_set(word) // the document inverse-frequence int hen post_content_corpus
			val tf = document.filter(_ == word).length // the number of occurences of the word in the document.
			idf * tf
		} else 0
	}.toSeq
})


val title_corpus = (indexed
	.select("title_keywords")
	.collect()
	.map{row => row.getAs[Seq[String]](0)}
)

var unique_title_set = Map.empty[String, Double]

title_corpus.foreach( document => {
	document.toSeq.map(word => unique_title_set += (word -> 0))
})

unique_title_set.toSeq.foreach(kv_pair => {
	val word = kv_pair._1
	var df = title_corpus.filter(_.contains(word)).length
	unique_title_set += (word -> log( (title_corpus.length) / (df + 1) ))
})

val title_tfidfVector = udf[Seq[Double], Seq[String]]( (document: Seq[String]) => {
	unique_title_set.map{ kv_pair => 
		val word = kv_pair._1
		if(document.contains(word)){
			var idf = unique_title_set(word)
			var tf = document.filter(_ == word).length
			idf * tf
		} else 0
	}.toSeq
})


// println(unique_post_content_set)

// val getHashingTrick = udf[Seq[Int], Seq[String]]((document: Seq[String]) => {
// 	// unique_post_content_set.toSeq.map{ word => if (document.contains(word)) 1 else 0}
// 	unique_post_content_set.map{ kv_pair => if (document.contains(kv_pair._1)) 1 else 0}.toSeq
// })

val indexedWithHash = (indexed
	// .withColumn("post_content_hash", getHashingTrick($"post_content_keywords"))
	.withColumn("post_content_tfidf", post_content_tfidfVector($"post_content_keywords"))
	.withColumn("title_tfidf", title_tfidfVector($"title_keywords"))
)


// readup
// val hashingTF = new HashingTF()
// HashingTF creates a new dataset and does not add a new column to the original dataset
// val tf: RDD[Vector] = hashingTF.transform(indexed.select("post_content_keywords"))

indexedWithHash.show(10)
// tf.show(10)

// create a global object.
// map through the corpus, and return the
// udf - will receive the blog content of the post, and iterate over the set that was created.
// if the word in the 
// create a new column will contain an array of indices

// https://spark.apache.org/docs/1.1.1/api/scala/index.html#org.apache.spark.mllib.feature.HashingTF
// https://spark.apache.org/docs/1.1.1/mllib-feature-extraction.html#tf-idf
// https://spark.apache.org/docs/2.1.0/mllib-feature-extraction.html#tf-idf
// https://spark.apache.org/docs/latest/mllib-feature-extraction.html#tf-idf

// cosine similarity




// https://spark.apache.org/docs/2.1.0/ml-features.html#stringindexer
// http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
// https://spark.apache.org/docs/1.3.0/api/java/org/apache/spark/SparkContext.html
// https://en.wikipedia.org/wiki/N-gram
// http://www.cmpbjournal.com/article/S0169-2607(05)00236-1/fulltext?cc=y
// https://www.microsoft.com/en-us/research/publication/use-clustering-techniques-language-modeling-application-asian-languages/




