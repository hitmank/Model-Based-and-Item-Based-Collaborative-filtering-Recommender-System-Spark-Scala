import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.NaNvl

import scala.collection.mutable.ListBuffer

object user_and_item_based{
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime

    val mode : String = "itemBased";


    val sparkConf = new SparkConf().setAppName("UserItem_CF").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(args(0))
    val ratings = data.map(_.split(',').dropRight(1))
    sc.setCheckpointDir("rddCheckpoint")
    data.checkpoint()

    //remove header
    val train_header = ratings.first()
    val pre_training = ratings.filter(elem=>elem.deep != train_header.deep)
    //---------------
    //Genre information for imputation

//    val genreFile = sc.textFile("data/movies.csv")
//    val removed_Col_2 = genreFile.map(elem=>{
//      val split = elem.split(",")
//      (split(0).toInt,split(1))
//    })
//    val movie_to_genre_map = removed_Col_2.collectAsMap()

    //----------------

    //get test set
    val unfiltered_dev_test = sc.textFile(args(1)).map(_.split(','))
    val test_header = unfiltered_dev_test.first()
    var testing = unfiltered_dev_test.filter(elem=>elem.deep != test_header.deep)


    val test_map  = testing.collect().map(d=>((d(0).toInt,d(1).toInt),0)).toMap
    val train_map = pre_training.collect().map(d=>((d(0).toInt,d(1).toInt),d(2).toDouble))

    // finally
    val filtered_training = train_map.filterNot(x=>(test_map.contains((x._1))))
    val filtered_training_map = filtered_training.toMap
    val testing_withAnswers = train_map.filter(x=>test_map.contains(x._1)).toMap

    val user_to_movies = filtered_training.map(elem=>elem._1)
    val user_to_movies_rdd = sc.parallelize(user_to_movies).groupByKey()
    val user_to_movies_map = user_to_movies_rdd.collectAsMap()

    val movies_to_user = filtered_training.map(elem=>(elem._1._2,elem._1._1))
    val movies_to_user_rdd = sc.parallelize(movies_to_user).groupByKey()
    val movie_to_user_map = movies_to_user_rdd.collectAsMap()

    var itemSimilarities : scala.collection.mutable.Map[(Int,Int),Double] = scala.collection.mutable.Map[(Int,Int),Double]()
    def getSimilarityBetweenItems(item1: Int, item2: Int) : Double ={

      var itemA : Int = item1
      var itemB : Int = item2
      if(itemA > itemB){
        itemA = item2
        itemB = item1
      }

      if(itemSimilarities.contains((itemA,itemB))){
        return itemSimilarities((itemA,itemB))
      }

      //Need to get all users who have rated both items
      if(!movie_to_user_map.contains(item1)){
        itemSimilarities((item1,item2)) = 0.0
        return 0.0
      }
      val users_who_have_rated_item1 = movie_to_user_map(item1).toSet
      val users_who_have_rated_item2 = movie_to_user_map(item2).toSet

      val users_who_have_rated_both = users_who_have_rated_item1.intersect(users_who_have_rated_item2)

      //Now, need to find the average rating for both items among these common users
      var sum_of_all_ratings_for_item1 : Double = 0.0
      var sum_of_all_ratings_for_item2 : Double = 0.0

      users_who_have_rated_both.map(elem=>{
        sum_of_all_ratings_for_item1 += filtered_training_map(elem,item1)
        sum_of_all_ratings_for_item2 += filtered_training_map(elem,item2)
      })

      val item1_average_rating = sum_of_all_ratings_for_item1 / users_who_have_rated_both.size
      val item2_average_rating = sum_of_all_ratings_for_item2 / users_who_have_rated_both.size

      var numerator : Double = 0.0
      var denominator : Double = 0.0
      var denominator_left : Double = 0.0
      var denominator_right : Double = 0.0


      users_who_have_rated_both.map(elem=>{
        numerator += (filtered_training_map(elem,item1) - item1_average_rating) * (filtered_training_map(elem,item2) - item2_average_rating)
        denominator_left += math.pow((filtered_training_map(elem,item1) - item1_average_rating),2)
        denominator_right += math.pow((filtered_training_map(elem,item2) - item2_average_rating),2)
      })

      denominator = math.sqrt(denominator_left) * math.sqrt(denominator_right)

      var weight : Double = 0.0;
      if(numerator != 0.toDouble && denominator != 0.toDouble){
        weight = numerator/denominator
      }
      itemSimilarities((itemA,itemB)) = weight
      return weight
    }
    def getAverageRating(user: Int, rated_movies: Iterable[Int]): Double = {
      val numberOfMoviesRated = rated_movies.size;
      var cumulative_sum_of_ratings: Double = 0.0
      rated_movies.map(movie => {
        cumulative_sum_of_ratings += filtered_training_map(user, movie)
      })
      val avg = cumulative_sum_of_ratings / numberOfMoviesRated;
      return avg
    }
    def getCosineSimilarityBetweenItems(item1:Int, item2: Int) : Double = {

      var itemA : Int = item1
      var itemB : Int = item2
      if(itemA > itemB){
        itemA = item2
        itemB = item1
      }

      if(itemSimilarities.contains((itemA,itemB))){
        return itemSimilarities((itemA,itemB))
      }
      //Need to get all users who have rated both items
      if(!movie_to_user_map.contains(item1)){
        itemSimilarities((item1,item2)) = 0.0
        return 0.0
      }
      val users_who_have_rated_item1 = movie_to_user_map(item1).toSet
      val users_who_have_rated_item2 = movie_to_user_map(item2).toSet

      val users_who_have_rated_both = users_who_have_rated_item1.intersect(users_who_have_rated_item2)

      var numerator : Double = 0.0
      var denominator : Double = 0.0
      var denominator_left : Double = 0.0
      var denominator_right : Double = 0.0

      users_who_have_rated_both.map(user=>{

        numerator += filtered_training_map((user,item1)) * filtered_training_map((user,item2))
        denominator_left += filtered_training_map((user,item1))
        denominator_right += filtered_training_map((user,item2))


      })

      denominator = denominator_left * denominator_right
      var weight : Double = 0.0;
      if(numerator != 0.toDouble && denominator != 0.toDouble){
        weight = numerator/denominator
      }
      itemSimilarities((itemA,itemB)) = weight
      return weight
    }
    val user_to_his_average_rating = user_to_movies_rdd.map(elem => {

      val curr_user = elem._1
      val movies_he_has_rated = elem._2
      val averageRating = getAverageRating(curr_user, movies_he_has_rated)
      (curr_user, averageRating)

    }).collectAsMap()
    if(mode == "userBased") {






      val all_possible_pairs_of_users = user_to_his_average_rating.map(elem => elem._1).toArray.combinations(2)


      def getSimilarityBetweenUsers(user1: Int, user2: Int): Double = {

        val items_user1_has_rated = user_to_movies_map(user1).toSet
        val items_user2_has_rated = user_to_movies_map(user2).toSet

        val common_items = items_user1_has_rated.intersect(items_user2_has_rated)

        var numerator: Double = 0.0
        var denominator: Double = 0.0
        var denominator_part1: Double = 0.0
        var denominator_part2: Double = 0.0

        val user1_average_rating = user_to_his_average_rating(user1)
        val user2_average_rating = user_to_his_average_rating(user2)


        common_items.map(item => {

          numerator += (filtered_training_map(user1, item) - user1_average_rating) * (filtered_training_map(user2, item) - user2_average_rating)
          denominator_part1 += math.pow((filtered_training_map(user1, item) - user1_average_rating), 2)
          denominator_part2 += math.pow((filtered_training_map(user2, item) - user2_average_rating), 2)

        })

        denominator = math.sqrt(denominator_part1) * math.sqrt(denominator_part2)
        var final_similarity: Double = 0.0
        if (numerator != 0 && denominator != 0) {
          final_similarity = numerator / denominator
        }

        return final_similarity
      }


      val similarity_of_each_pair_of_users = all_possible_pairs_of_users.map(pair => {

        val user1 = pair(0)
        val user2 = pair(1)

        val similarity = getSimilarityBetweenUsers(user1, user2)

        (pair.toList.sorted, similarity)
      })


      //Time to do the predictions
      val movieToUser_map = movies_to_user_rdd.collectAsMap()
      val similarity_map = similarity_of_each_pair_of_users.toMap

      def getAverageItemRating(item: Int): Double = {
        val users_who_have_rated = movieToUser_map(item)
        val g = users_who_have_rated.map(y => filtered_training_map(y, item)).sum

        return g / users_who_have_rated.size
      }

      def getPredictedRatingWithUserUserCF(user: Int, movie: Int, neighbourhoodSize: Int): Double = {


        //      val sim_of_active_user_with_all_others = k.filter(elem=>elem._1.contains(user)).toList.sortBy(x=> -x._2).take(neighbourhoodSize)
        if (movieToUser_map.contains(movie)) {
          val otherUsers_WhoHaveRatedTheMovie = movieToUser_map(movie)

          val sim_of_active_user_with_all_others = otherUsers_WhoHaveRatedTheMovie.map(elem => {

            var sim = 0.0
            if (user < elem) {
              sim = similarity_map(List(user, elem))
            }
            else {
              sim = similarity_map(List(elem, user))
            }
            (elem, sim)
          }).toList.sortBy(x => -x._2).take(neighbourhoodSize)


          var numerator: Double = 0.0
          var denominator: Double = 0.0

          sim_of_active_user_with_all_others.map(elem => {


            val theOtherUser = elem._1
            var theOtherUsersRatingOfThisMovie = filtered_training_map(theOtherUser, movie)
            val number_of_movies_other_user_has_rated = user_to_movies_map(theOtherUser).size
            var average_for_other_user_over_all_other_items = ((user_to_his_average_rating(theOtherUser) * number_of_movies_other_user_has_rated) - theOtherUsersRatingOfThisMovie) / number_of_movies_other_user_has_rated - 1


            numerator += (theOtherUsersRatingOfThisMovie - average_for_other_user_over_all_other_items) * elem._2
            denominator += math.abs(elem._2)


          })
          //      println(numerator)
          //      println(denominator)
          if (numerator == 0.toDouble || denominator == 0.toDouble) {
            return user_to_his_average_rating(user)
          }
          val finale = user_to_his_average_rating(user) + (numerator / denominator)

          return finale
        }
        else {
          return user_to_his_average_rating(user)
        }

      }

      val sd = testing_withAnswers.map(elem => {

        val user = elem._1._1
        val movie = elem._1._2

        val predictedRating = getPredictedRatingWithUserUserCF(user, movie, 1)

        (elem._1, elem._2, predictedRating)


      })


      // Item-Item


      val wala = sc.parallelize(sd.toSeq)
      val band1 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 0.toDouble && math.abs(elem._2 - elem._3) < 1.toDouble)).count()
      val band2 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 1.toDouble && math.abs(elem._2 - elem._3) < 2.toDouble)).count()
      val band3 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 2.toDouble && math.abs(elem._2 - elem._3) < 3.toDouble)).count()
      val band4 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 3.toDouble && math.abs(elem._2 - elem._3) < 4.toDouble)).count()
      val band5 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 4.toDouble)).count()


      println(">=0 and <1: " + band1)
      println(">=1 and <2: " + band2)
      println(">=2 and <3: " + band3)
      println(">=3 and <4: " + band4)
      println(">=4: " + band5)
      val MSE = wala.map(x => {
        val err = math.abs(x._2 - x._3);

        err * err
      }).mean()

      val RMSE = math.sqrt(MSE)
      println("RMSE = " + RMSE)
    }
    else if(mode == "itemBased"){

      val output = testing_withAnswers.map(elem=>{

        val user = elem._1._1;
        val movie = elem._1._2;

        val items_user_has_rated = user_to_movies_map(user)

        //of these items, find the most similar to the movie that needs to be predicted.
        val item_similarities = items_user_has_rated.map(item=>{
          val similarity = getSimilarityBetweenItems(movie,item)
          //val similarity = getCosineSimilarityBetweenItems(movie,item)
          (item,similarity)
        }).toList.sortBy(elem=> -elem._2).take(10)

        var numerator : Double = 0.0;
        var denominator : Double = 0.0;

        item_similarities.map(elem=>{

          numerator += filtered_training_map(user,elem._1) * elem._2
          denominator += math.abs(elem._2)

        })

        var predicted_rating : Double = 0.0
        if(numerator != 0.toDouble && denominator != 0.toDouble){
          predicted_rating = numerator/denominator;
          val sum = item_similarities.map(x=>x._2).sum
          val avgSim = sum/item_similarities.size

        }
        else{
//          predicted_rating = user_to_his_average_rating(elem._1._1)

//          var imputedMean = 0.0
//
//          val genreOfMovie = movie_to_genre_map(movie)
//
//          //getTheMovies of same genre that the user has rated
//
//          val all_movies_user_has_rated = user_to_movies_map(user)
//
//          val movies_user_has_rated_of_same_genre = all_movies_user_has_rated.filter(movieNo=>movie_to_genre_map(movieNo) == genreOfMovie)
//
//
//          if(movies_user_has_rated_of_same_genre.size > 0){
//            var sum = 0.0
//            movies_user_has_rated_of_same_genre.map(movieID=>{sum += filtered_training_map((user,movieID))})
//            imputedMean = sum/movies_user_has_rated_of_same_genre.size
//          }
//          else{
//            if(user_to_his_average_rating.contains(user)){
//              imputedMean =  user_to_his_average_rating(user)
//            }
////            else{
////              //          if(movie_to_user_map.contains(movie)){
////              //            imputedMean = getAverageItemRating(movie)
////              //          }
////              //          else{
////              imputedMean = get(user)
////              //          }
////              user_to_his_average_rating(user) = imputedMean
////            }
//          }

          predicted_rating = user_to_his_average_rating(user)
        }

        (elem._1,elem._2,predicted_rating)
      })

      sc.parallelize()
      val wala = sc.parallelize(output.toSeq)
      var toPrint : ListBuffer[(String,String,String)] = ListBuffer.empty
      val pred_format  = wala.map(elem=>(elem._1._1.toString,elem._1._2.toString,elem._3.toString)).sortBy(x=>(x._1.toInt,x._2.toInt))
      toPrint.append(("UserId","MovieId","Pred_rating"))
      val g =  pred_format.collect().map(elem=>{toPrint.append((elem._1,elem._2,elem._3))})
      g.size


      val print_rdd = sc.parallelize(toPrint)

      print_rdd.map(elem=>elem._1 + "," + elem._2 + "," + elem._3).coalesce(1,false).saveAsTextFile("Karan_Balakrishnan_result_task2")

      val band1 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 0.toDouble && math.abs(elem._2 - elem._3) < 1.toDouble)).count()
      val band2 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 1.toDouble && math.abs(elem._2 - elem._3) < 2.toDouble)).count()
      val band3 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 2.toDouble && math.abs(elem._2 - elem._3) < 3.toDouble)).count()
      val band4 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 3.toDouble && math.abs(elem._2 - elem._3) < 4.toDouble)).count()
      val band5 = wala.filter(elem => (math.abs(elem._2 - elem._3) >= 4.toDouble)).count()


      println(">=0 and <1: " + band1)
      println(">=1 and <2: " + band2)
      println(">=2 and <3: " + band3)
      println(">=3 and <4: " + band4)
      println(">=4: " + band5)
      val MSE = wala.map(x => {
        val err = math.abs(x._2 - x._3);

        err * err
      }).mean()

      val RMSE = math.sqrt(MSE)
      println("RMSE = " + RMSE)


    }


    val duration = (System.nanoTime - t1) / 1e9d
    println("The total execution time taken is " + duration + " secs")
  }
}