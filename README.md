# Spark-Querying
=======

> Toward Efficient SPARQL Queries on RDF Triple Stores using Apache Spark.

## Spark-Querying
SANSA Spark-Querying Library 

## Running the application on a Spark standalone cluster

To run the application on a standalone Spark cluster

1. Setup a Spark cluster
2. Build the application with Maven

  ```
  cd /path/to/application
  mvn clean package
  ```

3. Submit the application to the Spark cluster

  ```
  spark-submit \
		--class net.sansa_stack.querying.spark.App \
		--master spark://spark-master:7077 \
 		/app/application.jar \
		/data/input /data/output  
  ```

## Running the application on a Spark standalone cluster via Docker

To run the application, execute the following steps:

1. Setup a Spark cluster as described on http://github.com/big-data-europe/docker-spark.
2. Build the Docker image: 
`docker build --rm=true -t sansa/spark-querying .`
3. Run the Docker container: 
`docker run --name spark-querying-app -e ENABLE_INIT_DAEMON=false --link spark-master:spark-master  -d sansa/spark-querying`

## Running the application on a Spark standalone cluster via Spark/HDFS Workbench

Spark/HDFS Workbench Docker Compose file contains HDFS Docker (one namenode and two datanodes), Spark Docker (one master and one worker) and HUE Docker as an HDFS File browser to upload files into HDFS easily. Then, this workbench will play a role as for Spark-Querying application to perform computations.
Let's get started and deploy our pipeline with Docker Compose. 
Run the pipeline:

  ```
docker network create hadoop
docker-compose up -d
  ```
First, let’s throw some data into our HDFS now by using Hue FileBrowser runing in our network. To perform these actions navigate to http://your.docker.host:8088/home. Use “hue” username with any password to login into the FileBrowser (“hue” user is set up as a proxy user for HDFS, see hadoop.env for the configuration parameters). Click on “File Browser” in upper right corner of the screen and use GUI to create /user/root/input and /user/root/output folders and upload the data file into /input folder.
Go to http://your.docker.host:50070 and check if the file exists under the path ‘/user/root/input/yourfile.nt’.

After we have all the configuration needed for our example, let’s rebuild Spark-Querying.

```
docker build --rm=true -t sansa/spark-querying .
```
And then just run this image:
```
docker run --name spark-querying-app --net hadoop --link spark-master:spark-master \
-e ENABLE_INIT_DAEMON=false \
-d sansa/spark-querying
```


