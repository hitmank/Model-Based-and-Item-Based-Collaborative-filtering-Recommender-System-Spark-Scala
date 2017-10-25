# Model-Based-and-Item-Based-Collaborative-filtering-Recommender-System--Spark---Scala-
2 Recommender systems built on Spark with Scala 

# Scala version: 2.10 Spark version: 1.6.1 SBT version: 0.13.16

Task 1 :
# Model Based Recommender:
Accuracy:
>=0 and <1: 14018 >=1 and <2: 4812 >=2 and <3: 1081 >=3 and <4: 288 >=4: 57

RMSE = 1.0951547009088378

The total execution time taken is 20.503482597 secs

# How to execute the code:
Class name : model_based_recommender
I use 3 arguments :
1. Path to the ratings.csv file. This file is in the original downloaded state.
2. Path to my custom movies.csv file. This file is the modified version of the movies.csv file from the movielens_small dataset.
I use this file for my imputation. I have modified it to remove the 2nd column. The reason I have done this offline and not in code is because the file does not follow a standard format to separate the columns. Some lines have “(quotes) as delimiter and some have ,(comma). Hence to save from very time consuming string manipulation, I modified it offline and use that.
The modified file is called “movies_modified.csv” and is present in my submitted zip folder.
3. Path to the small_testing.csv file. Original state.
Command :
./bin/spark-submit --class model_based_recommender --master local[*] /Users/karanbalakrishnan/Desktop/Recommender_HW/Karan_Balakrishnan_task1.jar <path_to_ratings.csv> <path_to_modified_movies.csv> <path_to_small_testing.csv>
   
# EXAMPLE:
./bin/spark-submit --class model_based_recommender --master
local[*]
/Users/karanbalakrishnan/Desktop/Recommender_HW/Karan_Balakrishn
an_task1.jar
/Users/karanbalakrishnan/CF_ModelBased/data/ratings.csv
/Users/karanbalakrishnan/CF_ModelBased/data/movies_modified.csv
/Users/karanbalakrishnan/CF_ModelBased/data/testing_small.csv
# Description of what I have done:
1. Created the filtered training data after removing the testing data.
2. Trained the ALS algorithm with the filtered training data.
3. Used ALS model to predict the test data.
4. ALS does not predict for all the test cases due to cold-start problem. Hence I have
performed imputation based on this paper:
https://www.researchgate.net/publication/224587995_Boosting_collaborative_filtering _based_on_missing_data_imputation_using_item%27s_genre_information?enrichId=rg req-d11a21cbf649652e950d8e18916eddaf- XXX&enrichSource=Y292ZXJQYWdlOzIyNDU4Nzk5NTtBUzox
# Method for Imputation:
1. For <user,movie> in test that ALS could not predict, check the genre of the movie using the movie.csv file
2. Find all the other movies which this user has rated of the same genre.
3. Take average of only those movies.
4. Use this value for imputation.

Task 2:
# Item-Item based Collaborative filtering algorithm Accuracy:
>=0 and <1: 14119 >=1 and <2: 4854 >=2 and <3: 1102 >=3 and <4: 175 >=4: 6
         
RMSE = 1.0261703282676238

The total execution time taken is 77.251203738 secs

# How to execute the code:
Class name : user_and_item_based
I use 2 arguments :
1. Path to the ratings.csv file. This file is in the original downloaded state. 2. Path to the small_testing.csv file. Original state.
Command :
./bin/spark-submit --class user_and_item_based --master local[*] <path_to_jar> <path_to_ratings.csv> <path_to_small_testing.csv>
# EXAMPLE:
./bin/spark-submit --class user_and_item_based --master local[*] /Users/karanbalakrishnan/Desktop/Recommender_HW/Karan_Balakrishn an_task2.jar /Users/karanbalakrishnan/CF_ModelBased/data/ratings.csv /Users/karanbalakrishnan/CF_ModelBased/data/testing_small.csv
# Description of what I have done:
1. Created the filtered training data after removing the testing data.
2. Created various data structures that I would use later on. Such as : User to Movies
mapping, Movies to user mapping, User to his average rating mapping, Movie to its
average rating mapping etc.
3. I perform Item-Item based Collaborative filtering using Pearson Corelation
4. I used a neighbourhood of: 10
5. Item-Item based fails when there are no items that are common between users. I.e New
user or new item scenario.
6. In these cases I have performed a simple imputation
# Method for Imputation:
1. If not able to predict rating due to lack of users/items. 2. Use the users average rating.
