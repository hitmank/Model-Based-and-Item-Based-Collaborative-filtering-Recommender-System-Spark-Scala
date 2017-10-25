import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import scala.collection.mutable.ListBuffer

object model_based_recommender{
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val sparkConf = new SparkConf().setAppName("ModelBased_CF").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(args(0))
    val ratings = data.map(_.split(',').dropRight(1))
  sc.setCheckpointDir("hello")
    data.checkpoint()
    //remove header
    val train_header = ratings.first()
    val pre_training = ratings.filter(elem=>elem.deep != train_header.deep)


    //---------------
    //Genre information for imputation

    val genreFile = sc.textFile(args(1))
    val removed_Col_2 = genreFile.map(elem=>{
     val split = elem.split(",")
      (split(0).toInt,split(1))
    })
    val movie_to_genre_map = removed_Col_2.collectAsMap()




    //----------------





    //get test set
    val dev_test = sc.textFile(args(2)).map(_.split(','))
    val test_header = dev_test.first()
    var testing = dev_test.filter(elem=>elem.deep != test_header.deep)
    val test_map  = testing.collect().map(d=>((d(0).toInt,d(1).toInt),0)).toMap
    val train_map = pre_training.collect().map(d=>((d(0).toInt,d(1).toInt),d(2).toDouble))

    // finally
    val full_training = train_map.filterNot(x=>(test_map.contains((x._1))))
    val full_testing = train_map.filter(x=>test_map.contains(x._1))
    val collection_testing = full_testing.map(x=>Rating(x._1._1.toInt,x._1._2.toInt,x._2.toDouble))

    val user_to_movies = full_training.map(elem=>elem._1)
    val user_to_movies_rdd = sc.parallelize(user_to_movies.toSeq).groupByKey()
    val user_to_movies_map = user_to_movies_rdd.collectAsMap()
    val full_training_intified = full_training.toMap


    var user_average_rating = scala.collection.mutable.Map[Int,Double]()
    var item_average_rating = scala.collection.mutable.Map[Int,Double]()

    val movies_to_user = full_training_intified.map(elem=>(elem._1._2,elem._1._1))
    val movies_to_user_rdd = sc.parallelize(movies_to_user.toSeq).groupByKey()
    val movie_to_user_map = movies_to_user_rdd.collectAsMap()
    def getAverageItemRating(item: Int): Double = {

      val users_who_have_rated = movie_to_user_map(item)


      val g = users_who_have_rated.map(y => full_training_intified(y, item)).sum

      return g / users_who_have_rated.size
    }

    def getAverageRatingForUser(user : Int) : Double = {
      val rated_movies = user_to_movies_map(user)

      val numberOfMoviesRated = rated_movies.size;
      var cumulative_sum_of_ratings: Double = 0.0
      rated_movies.map(movie => {
        cumulative_sum_of_ratings += full_training_intified(user, movie)
      })
      val avg = cumulative_sum_of_ratings / numberOfMoviesRated;
      return avg
    }

    val collection_rating = full_training.map(x=>Rating(x._1._1.toInt,x._1._2.toInt,x._2.toDouble))
//
//    val all_possible_user_to_movie_combinations_temp = movie_to_user_map.keys.map(movie=>user_to_movies_map.keys.map(user=>(user,movie)))
//    val all_possible_user_to_movie_combinations = all_possible_user_to_movie_combinations_temp.flatten
//
//    val real_collection_rating = all_possible_user_to_movie_combinations.map(elem=>{
//
//
//      val user = elem._1;
//      val movie = elem._2;
//
//      var rat : Double = 0.0
//
//      if(full_training_intified.contains((user,movie))){
//        rat = full_training_intified((user,movie))
//      }
//      else{
//
//        if(user_average_rating.contains(user)){
//          rat = user_average_rating(user)
//        }
//        else{
//          rat = getAverageRatingForUser(user)
//          user_average_rating(user) = rat
//        }
//
//
//      }
//
//      Rating(user,movie,rat)
//
//
//
//
//
//    })
//




    val train_rating = sc.parallelize(collection_rating.toSeq)
    val rank = 5
    val numIterations = 4

    val model = ALS.train(train_rating,rank,numIterations)

    val toTest = testing.collect().map(d=>(d(0),d(1)))

    val s = toTest.map(x=>(x._1.toInt -> x._2.toInt))
    val test_rdd = sc.parallelize(s.toSeq)
    test_rdd.checkpoint()
    val predicitiosn = model.predict(test_rdd).map { case Rating(user, product, rate) =>
              ((user, product), rate)
            }
    predicitiosn.checkpoint()


    val ratesAndPreds = collection_testing.map { case Rating(user, product, rate) =>
          ((user, product), rate)
    }
    val predicition_map = predicitiosn.collectAsMap()
    val test_cases_which_didnt_get_predicted = ratesAndPreds.filter(elem=> !predicition_map.contains(elem._1))



    val imputed_ratings = test_cases_which_didnt_get_predicted.map(elem=>{
      val user = elem._1._1;
      val movie = elem._1._2;
      var imputedMean = 0.0

      val genreOfMovie = movie_to_genre_map(movie)

      //getTheMovies of same genre that the user has rated

      val all_movies_user_has_rated = user_to_movies_map(user)

      val movies_user_has_rated_of_same_genre = all_movies_user_has_rated.filter(movieNo=>movie_to_genre_map(movieNo) == genreOfMovie)


      if(movies_user_has_rated_of_same_genre.size > 0){
        var sum = 0.0
        movies_user_has_rated_of_same_genre.map(movieID=>{sum += full_training_intified((user,movieID))})
        imputedMean = sum/movies_user_has_rated_of_same_genre.size
      }
      else{
        if(user_average_rating.contains(user)){
          imputedMean =  user_average_rating(user)
        }
        else{
//          if(movie_to_user_map.contains(movie)){
//            imputedMean = getAverageItemRating(movie)
//          }
//          else{
            imputedMean = getAverageRatingForUser(user)
//          }
          user_average_rating(user) = imputedMean
        }
      }
      (elem._1,imputedMean)



      //      //check how many users have rated the movie. = x
//      //check how many movies a user has rated. = y
//
//      //using x and y, come up with a switcher
//
//
//
//
//
//      if(item_average_rating.contains(movie)){
//        imputedMean =  item_average_rating(movie)
//      }
//      else{
//        if(movie_to_user_map.contains(movie)){
//          imputedMean = getAverageItemRating(movie)
//        }
//        else{
//          imputedMean = getAverageRatingForUser(user)
//        }
//        item_average_rating(movie) = imputedMean
//      }
//      (elem._1,imputedMean)
    }).toMap

    val final_prediction = predicition_map ++ imputed_ratings
    val rdds_prediction = sc.parallelize(final_prediction.toSeq)


    var toPrint : ListBuffer[(String,String,String)] = ListBuffer.empty
    val pred_format  = rdds_prediction.map(elem=>(elem._1._1.toString,elem._1._2.toString,elem._2.toString)).sortBy(x=>(x._1.toInt,x._2.toInt))
    toPrint.append(("UserId","MovieId","Pred_rating"))
    val g =  pred_format.collect().map(elem=>{toPrint.append((elem._1,elem._2,elem._3))})
    g.size


    val print_rdd = sc.parallelize(toPrint)

    print_rdd.map(elem=>elem._1 + "," + elem._2 + "," + elem._3).coalesce(1,false).saveAsTextFile("Karan_Balakrishnan_result_task1")



    val rdds = sc.parallelize(ratesAndPreds.toSeq)


    val wala = rdds.join(rdds_prediction)

    val band1 = wala.filter(elem=>( math.abs(elem._2._2 - elem._2._1) >= 0.toDouble &&  math.abs(elem._2._2 - elem._2._1) < 1.toDouble )).count()
    val band2 = wala.filter(elem=>( math.abs(elem._2._2 - elem._2._1) >= 1.toDouble &&  math.abs(elem._2._2 - elem._2._1) < 2.toDouble )).count()
    val band3 = wala.filter(elem=>( math.abs(elem._2._2 - elem._2._1) >= 2.toDouble &&  math.abs(elem._2._2 - elem._2._1) < 3.toDouble )).count()
    val band4 = wala.filter(elem=>( math.abs(elem._2._2 - elem._2._1) >= 3.toDouble &&  math.abs(elem._2._2 - elem._2._1) < 4.toDouble )).count()
    val band5 = wala.filter(elem=>( math.abs(elem._2._2 - elem._2._1) >= 4.toDouble)).count()





    val MSE = wala.map{case((u,p),(r1,r2)) =>{val err = math.abs(r2 - r1);err * err;}}.mean()
    val RMSE = math.sqrt(MSE)
    val duration = (System.nanoTime - t1) / 1e9d


    println(">=0 and <1: " + band1)
    println(">=1 and <2: " + band2)
    println(">=2 and <3: " + band3)
    println(">=3 and <4: " + band4)
    println(">=4: " + band5)
    println("RMSE = " + RMSE)
    println("The total execution time taken is " + duration + " secs")
  }
}