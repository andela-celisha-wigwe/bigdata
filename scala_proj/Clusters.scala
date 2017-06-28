import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.math.{log, sqrt}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, lit, desc, sum, unix_timestamp, lower, size}
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

// val dateToTS = unix_timestamp($"date_created", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
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
	// .withColumn("ts", dateToTS)
	.withColumn("author", lower($"author"))
	.withColumn("my_author_index", getAuthorIndex($"author"))
).cache()

val my_concat = udf[Seq[String], Seq[String], Seq[String]]((vector1: Seq[String], vector2: Seq[String]) => vector1 ++ vector2)

val pipeline = new Pipeline().setStages(Array(authorIndexer, postURLIndexer, post_content_tokenizer, post_content_stop_words_remover, post_content_ngrammer, title_tokenizer, title_stop_words_remover, title_ngrammer))
val indexed = (pipeline.
	fit(data).
	transform(data)
	.withColumn("important_keywords", my_concat($"title_keywords", $"post_content_keywords"))
	.cache())

	val keywords_corpus = (indexed
		.select("important_keywords")
		.collect()
		.map{row => row.getAs[Seq[String]] (0)}
	)

	// var unique_keyword_set = Set.empty[String]
	var unique_keyword_set = Map.empty[String, Double]

	keywords_corpus.foreach((document) => {
		// document.toSeq.foreach(word => unique_keyword_set += word.toString())
		document.toSeq.map(word => unique_keyword_set += (word -> 0))
	})

	unique_keyword_set.toSeq.foreach((kv_pair) => {
		val word = kv_pair._1
		val df = keywords_corpus.filter(_.contains(word)).length
		// unique_keyword_set = unique_keyword_set.set(word, log((keywords_corpus.length) / (df + 1)))
		unique_keyword_set += (word -> log((keywords_corpus.length) / (df + 1))) // plus 1 => if df is 0, we'll get an error.
	})

	val keywords_tfidfVector = udf[Seq[Double], Seq[String]]((document: Seq[String]) => {
		unique_keyword_set.map{ kv_pair => 
			val word = kv_pair._1
			if (document.contains(word)) {
				val idf = unique_keyword_set(word) // the document inverse-frequence int hen keywords_corpus
				val tf = document.filter(_ == word).length // the number of occurences of the word in the document.
				idf * tf
			} else 0
		}.toSeq
	})

	// val post_content_corpus = (indexed
	// 	.select("post_content_keywords")
	// 	.collect()
	// 	.map{row => row.getAs[Seq[String]](0)}
	// )
	// // println(post_content_corpus)

	// // var unique_post_content_set = Set.empty[String]
	// var unique_post_content_set = Map.empty[String, Double]

	// post_content_corpus.foreach((document) => {
	// 	// document.toSeq.foreach(word => unique_post_content_set += word.toString())
	// 	document.toSeq.map(word => unique_post_content_set += (word -> 0))
	// })

	// unique_post_content_set.toSeq.foreach((kv_pair) => {
	// 	val word = kv_pair._1
	// 	val df = post_content_corpus.filter(_.contains(word)).length
	// 	// unique_post_content_set = unique_post_content_set.set(word, log((post_content_corpus.length) / (df + 1)))
	// 	unique_post_content_set += (word -> log((post_content_corpus.length) / (df + 1))) // plus 1 => if df is 0, we'll get an error.
	// })

	// val post_content_tfidfVector = udf[Seq[Double], Seq[String]]((document: Seq[String]) => {
	// 	unique_post_content_set.map{ kv_pair => 
	// 		val word = kv_pair._1
	// 		if (document.contains(word)) {
	// 			val idf = unique_post_content_set(word) // the document inverse-frequence int hen post_content_corpus
	// 			val tf = document.filter(_ == word).length // the number of occurences of the word in the document.
	// 			idf * tf
	// 		} else 0
	// 	}.toSeq
	// })


// val title_corpus = (indexed
// 	.select("title_keywords")
// 	.collect()
// 	.map{row => row.getAs[Seq[String]](0)}
// )

// var unique_title_set = Map.empty[String, Double]

// title_corpus.foreach( document => {
// 	document.toSeq.map(word => unique_title_set += (word -> 0))
// })

// unique_title_set.toSeq.foreach(kv_pair => {
// 	val word = kv_pair._1
// 	var df = title_corpus.filter(_.contains(word)).length
// 	unique_title_set += (word -> log( (title_corpus.length) / (df + 1) ))
// })

// val title_tfidfVector = udf[Seq[Double], Seq[String]]( (document: Seq[String]) => {
// 	unique_title_set.map{ kv_pair => 
// 		val word = kv_pair._1
// 		if(document.contains(word)){
// 			var idf = unique_title_set(word)
// 			var tf = document.filter(_ == word).length
// 			idf * tf
// 		} else 0
// 	}.toSeq
// })



val euclidean_norm = udf[Double, Seq[Double]]((tf_idf: Seq[Double]) => {
	sqrt(tf_idf.map(v => v * v).sum)
})


// println(unique_post_content_set)

// val getHashingTrick = udf[Seq[Int], Seq[String]]((document: Seq[String]) => {
// 	// unique_post_content_set.toSeq.map{ word => if (document.contains(word)) 1 else 0}
// 	unique_post_content_set.map{ kv_pair => if (document.contains(kv_pair._1)) 1 else 0}.toSeq
// })

val indexedWithHash = (indexed
	// .withColumn("post_content_hash", getHashingTrick($"post_content_keywords"))
	
	// .withColumn("post_content_tfidf", post_content_tfidfVector($"post_content_keywords"))
	// .withColumn("post_content_euclidean_norm", euclidean_norm($"post_content_tfidf"))
	
	// .withColumn("title_tfidf", title_tfidfVector($"title_keywords"))
	// .withColumn("title_euclidean_norm", euclidean_norm($"title_tfidf"))

	.withColumn("keywords_tfidf", keywords_tfidfVector($"important_keywords"))
	.withColumn("keywords_euclidean_norm", euclidean_norm($"keywords_tfidf"))

).cache()

val corpus_of_keywords_tfidf = indexedWithHash.select("keywords_tfidf").collect().map(row => row.getAs[Seq[Double]](0))
val corpus_of_keywords_euclidean_norm = indexedWithHash.select("keywords_euclidean_norm").collect().map(row => row.getAs[Double](0))

// val corpus_of_post_content_tfidf = indexedWithHash.select("post_content_tfidf").collect().map(row => row.getAs[Seq[Double]](0))
// val corpus_of_post_content_euclidean_norm = indexedWithHash.select("post_content_euclidean_norm").collect().map(row => row.getAs[Double](0))

// cosine_similarity will be called for every document
val cosine_similarity = udf[Seq[Double], Seq[Double], Double]((documentTfidfVector: Seq[Double], documentEuclidianNorm: Double) => {
	def dotProduct(vector1: Seq[Double], vector2: Seq[Double]): Double = {
		// (0 to (vector1.size() - 1)).map((i: Int) => vector1(i) * vector2(i)).sum
		(vector1 zip vector2).map{(pair: (Double, Double)) => pair._1 * pair._2 }.sum
	}

	(corpus_of_keywords_euclidean_norm zip corpus_of_keywords_tfidf).map{ pair =>
	  val tfidfVector = pair._2
	  val euclidianNorm = pair._1
	  dotProduct(documentTfidfVector, tfidfVector) / (euclidianNorm * documentEuclidianNorm)
	}

	// (0 to euclidean_norm_corpus.length).map { i =>
	//   val tfidfVector = tf_idf_corpus(i)
	//   val euclidianNorm = euclidean_norm_corpus(i)
	  
	//   dotProduct(documentTfidfVector, tfidfVector) / (euclidianNorm * documentEuclidianNorm)
	// }
})

// val post_with_max_cosine = udf[Seq[String], Seq[Double]]((cosine_sim_seq: Seq[Double]) => {
// 	val index_with_max = (cosine_sim_seq zip (0 to cosine_sim_seq.length)).maxBy(_._1)._2
// 	keywords_corpus(index_with_max)
// })

// val experiUDF = udf[Seq[String], Seq[Double]]( (cosine_seq: Seq[Double]) => {
// 	val index_with_max = ((0 to cosine_seq.filter(_<0.9999).length) zip cosine_seq.filter(_<0.9999)).maxBy(_._2)._1
// 	keywords_corpus(index_with_max)
// })

val experiUDF = udf[Seq[String], Seq[Double]]( (cosine_seq: Seq[Double]) => { keywords_corpus(((0 to cosine_seq.filter(_<0.9999).length) zip cosine_seq.filter(_<0.9999)).maxBy(_._2)._1) } )

val removeNaN = udf[Seq[Double], Seq[Double]]((doc) => {
	doc.map((d: Double) => {
		if (d.isNaN || d > 0.99) {
			0.0
		} else {
			d
		}
	})
})

val dfWithCosine = (indexedWithHash
	.withColumn("keywords_cosine_similarity", cosine_similarity($"keywords_tfidf", $"keywords_euclidean_norm"))
	.withColumn("keywords_cosine_similarity_without_nan", removeNaN($"keywords_cosine_similarity"))
).cache()

// val dfWithoutNaN = dfWithCosine.select($"keywords_cosine_similarity").collect().foreach((doc) => {
// 	doc.getAs[Seq[Double]](0).map((d: Double) => {if (d.isNaN) 0.0 else d});
// })

// val newDataframe = (dfWithCosine
// 	// .filter(size($"keywords_cosine_similarity") > 0)
// 	.withColumn("closest_post_content", experiUDF($"keywords_cosine_similarity"))
// 	// .withColumn("post_content_cosine_similarity", cosine_similarity($"post_content_tfidf", $"post_content_euclidean_norm", corpus_of_post_content_euclidean_norm, corpus_of_post_content_tfidf))
// ).cache()

val newDataframe = dfWithCosine.withColumn("experiment", experiUDF($"keywords_cosine_similarity_without_nan")).cache()

newDataframe.show(10)


// readup
// val hashingTF = new HashingTF()
// HashingTF creates a new dataset and does not add a new column to the original dataset
// val tf: RDD[Vector] = hashingTF.transform(indexed.select("post_content_keywords"))

// indexedWithHash.show(10)

// val adder = udf[Double, Seq[Double]]((array_double: Seq[Double]) => array_double.sum)

// indexedWithHash.withColumn("sum_of_title", adder($"title_tfidf")).show(10)


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



// K-Means Clustering
// https://en.wikipedia.org/wiki/K-means_clustering#Algorithms
// https://spark.apache.org/docs/latest/mllib-clustering.html#k-means

// try to remove the column with 0.99999 and make an attmept again.

