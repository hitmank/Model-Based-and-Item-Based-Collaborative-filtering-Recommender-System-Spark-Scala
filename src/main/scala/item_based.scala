import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import scala.collection.mutable.ListBuffer

object task2{
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val sparkConf = new SparkConf().setAppName("ModelBased_CF").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile("data/ratings.csv")
    val ratings = data.map(_.split(',').dropRight(1))
    sc.setCheckpointDir("hello")
    data.checkpoint()
    //remove header
    val train_header = ratings.first()
    val pre_training = ratings.filter(elem=>elem.deep != train_header.deep)


    //get test set
    val dev_test = sc.textFile("data/testing_small.csv").map(_.split(','))
    val test_header = dev_test.first()
    var testing = dev_test.filter(elem=>elem.deep != test_header.deep)


    val test_map  = testing.collect().map(d=>((d(0),d(1)),0)).toMap
    val train_map = pre_training.collect().map(d=>((d(0),d(1)),d(2)))

    // finally
    val filtered_training = train_map.filterNot(x=>(test_map.contains((x._1))))

    val user_to_movies = filtered_training.map(elem=>elem._1)
    val user_to_movies_rdd = sc.parallelize(user_to_movies).groupByKey().collectAsMap()


    val full_testing = train_map.filter(x=>test_map.contains(x._1)).toMap

//    println(filtered_training.size)
//    var se = scala.collection.mutable.Set[Int]()
//    filtered_training.map(x=> se.add(x._1._2.toInt))
//    println(se.size)

    val userToMovieRating = filtered_training.map(elem=>(elem._1._1.toInt,(elem._1._2.toInt,elem._2.toDouble)))
    val userMovieToRating = filtered_training.map(elem=>((elem._1._1.toInt,elem._1._2.toInt),elem._2.toDouble)).toMap

    val paa = sc.parallelize(userToMovieRating.toSeq)
    val userToMovieRating_joined  = paa.join(paa);

//    //manage the duplicates
    val cleaned_userToMovieRating_joined = userToMovieRating_joined.filter(elem=>{

      elem._2._1._1 < elem._2._2._1


    })

//    cleaned_userToMovieRating_joined.take(20).foreach(println)

    val thefinale  = cleaned_userToMovieRating_joined.map(elem=>{

      ((elem._2._1._1,elem._2._2._1),elem._1)



    })
    val corated_users = thefinale.groupByKey()


//    println(corated_users.count())
//    corated_users.take(30).foreach(x=>println(x))


    val similarity_between_items = corated_users.map(elem=>{

      val item1 = elem._1._1
      val item2 = elem._1._2

      val users_who_have_rated_both_items = elem._2

      var average_item1 : Double  = 0.0
      var average_item2  : Double = 0.0

      users_who_have_rated_both_items.map(elem=>{

          average_item1  += userMovieToRating((elem,item1))
          average_item2  += userMovieToRating((elem,item2))

      })

      average_item1 = average_item1/users_who_have_rated_both_items.size
      average_item2 = average_item2/users_who_have_rated_both_items.size

      var numerator : Double = 0.0;
      var denominator_left : Double  = 0.0;
      var denominator_right : Double  = 0.0;
      users_who_have_rated_both_items.map(elem=>{

        numerator += (userMovieToRating((elem,item1)) - average_item1)*(userMovieToRating((elem,item2)) - average_item2)
        denominator_left += scala.math.pow((userMovieToRating((elem,item1)) - average_item1),2)
        denominator_right += scala.math.pow((userMovieToRating((elem,item2)) - average_item2),2)


      })


      denominator_left = math.sqrt(denominator_left)
      denominator_right = math.sqrt(denominator_right)

      val denominator = (denominator_left * denominator_right).toDouble;

      var sim = 0.0;
      if (!(numerator == 0.toDouble || denominator == 0.toDouble)){
        sim = numerator/denominator;

      }

      (elem._1,sim)




    }).collectAsMap()

    println("ahahahah =" + similarity_between_items.size)

//okay now work on the test set.
    val toTest = testing.collect().map(d=>(d(0),d(1)))

    val neighbourhoodSize = 2;

//    val predictedOutPutandRealOutput = toTest.map(elem=>{
//
//      val user = elem._1
//      val movie = elem._2
//      val movies_this_user_has_rated = user_to_movies_rdd(user)
//      var numberOfItems = neighbourhoodSize;
//      if (movies_this_user_has_rated.size < neighbourhoodSize){
//        numberOfItems = movies_this_user_has_rated.size
//      }
//
//      var N_most_similar_movies : ListBuffer[String] = ListBuffer.empty
//      val Daratings = movies_this_user_has_rated.map(x=>{
//        var curr_sim : Double = -9999999.0;
//        if (movie.toInt < x.toInt) {
//          if (similarity_between_items.contains(movie.toInt,x.toInt)) {
//            curr_sim = similarity_between_items(movie.toInt, x.toInt)
//          }
//        }
//        else{
//          if (similarity_between_items.contains(movie.toInt,x.toInt)) {
//            curr_sim = similarity_between_items(x.toInt,movie.toInt)
//          }
//
//        }
//          (curr_sim, x)
//
//      })
//
//      val sorted = Daratings.toList.sortBy(e=>e._1).reverse
//
//      var numberOfItemsDone = 0;
//      var predictedRating : Double= 0.0;
//      var num : Double = 0.0;
//      var den : Double = 0.0;
//      do{
//
//        num = num + (userMovieToRating(user.toInt,sorted(numberOfItems)._2.toInt) * sorted(numberOfItems)._1)
//        den = den + sorted(numberOfItems)._1;
//        numberOfItemsDone += 1
//      }while(numberOfItemsDone<numberOfItems && numberOfItemsDone+1 < sorted.size);
//
//
//      if(num != 0.toDouble && den != 0.toDouble){
//        predictedRating = num/den;
//      }
//
//
//
//      (elem,predictedRating,full_testing(elem).toDouble)
//
//
//
//
//
//
//
//    })

//    val fffinal = sc.parallelize(predictedOutPutandRealOutput)

//    println(predictedOutPutandRealOutput.size)
//    predictedOutPutandRealOutput.take(10).foreach(println)

    //calculate RMSE

//    val band1 = fffinal.filter(elem=>( math.abs(elem._3 - elem._2) >= 0.toDouble &&  math.abs(elem._3 - elem._2) < 1.toDouble )).count()
//    val band2 = fffinal.filter(elem=>( math.abs(elem._3 - elem._2) >= 1.toDouble &&  math.abs(elem._3 - elem._2) < 2.toDouble )).count()
//    val band3 = fffinal.filter(elem=>( math.abs(elem._3 - elem._2) >= 2.toDouble &&  math.abs(elem._3 - elem._2) < 3.toDouble )).count()
//    val band4 = fffinal.filter(elem=>( math.abs(elem._3 - elem._2) >= 3.toDouble &&  math.abs(elem._3 - elem._2) < 4.toDouble )).count()
//    val band5 = fffinal.filter(elem=>( math.abs(elem._3 - elem._2) >= 4.toDouble)).count()



//    println(">=0 and <1: " + band1)
//    println(">=1 and <2: " + band2)
//    println(">=2 and <3: " + band3)
//    println(">=3 and <4: " + band4)
//    println(">=4: " + band5)
//
//    val MSE = fffinal.map{case((u,p),r1,r2) =>{val err = r2 - r1;err * err;}}.mean()
//    val RMSE = math.sqrt(MSE)
//    println("RMSE = " + RMSE)
//
//    fffinal.take(30).foreach(println)

  }
}