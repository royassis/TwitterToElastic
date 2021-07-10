# TwitterToElastic (Twitter To Elastic)

## Description 
Course project from [Apache Kafka Series - Learn Apache Kafka for Beginners v2][kafka-course] Udemy course.

The projects goal is to pull tweeter tweets using the [HouseBirdClient package][housebird-client-repo] and the  
tweeter API.   

Then, push these tweets into a kafka topic.

Later on, read these tweets using a kafka consumer and push them into elastic-search using a kafka producer.

Finally, kafka streaming API and kafka connect, was used instead of the consumer and produer API to achive same results.

## Modules
This java project was broken down into moduls, each module represent a different stage of progress during the project.

- KafkaInsertIntoElasticSearch
- StreamTwitterToKafka
- TweeterWithKafkaStreams
- TwitterToTerminal

##### KafkaInsertIntoElasticSearch
Read data from kafka topic into elastic search, using the producer/consumer API.

##### StreamTwitterToKafka
Using the producer/consumer API, read tweets from using housebird client into kafka topic. 
Code is formatted better. 
  
##### TweeterWithKafkaStreams
Using kafka streaming API, reads tweets from one topic, filters and inserts into another, based on some key value of tweet.
  
##### TwitterToTerminal
Get tweets using the housebird client and inserts into a kafka topic. 

## Cavets 
#### Tweeter API
In order to run this project you'll need to register a tweeter client account, and 
place a json config file at project root named creds.json. The file should contain the following keys:
- apiKey 
- apiSecretKey
- accessToken
- accessTokenSecret

All of which are provided on registration of a new tweeter api client account. 

#### KAFKA
In order to run this project you'll need kafka installed and running 

#### Changes to be made
- Some files contains hard coded ip's and ports to kafka, in order to run, refractor or insert correct values.
- Some files read creds.json from another location. 



[kafka-course]:https://www.udemy.com/course/apache-kafka/
[housebird-client-repo]:https://github.com/twitter/hbc
[java-elastic]:https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.13/index.html