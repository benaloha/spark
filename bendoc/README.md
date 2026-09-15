Ontwikkel:

bash:
sbt

sbt-cli (snelste delta compiles)
project example
package

bash:
./bin/run-example JavaLogQuery /mnt/spark/access.log 1 20


Cluster:

cp ~\git\spark\examples\target\scala-2.13\jars\spark-examples_2.13-5.0.0-SNAPSHOT.jar /mnt/spark/

docker swarm init --advertise-addr 192.168.178.42

docker stack deploy -c docker-swarm.yml spark-cluster

docker stack rm spark-cluster


docker exec -it $(docker ps -q -f name=spark-cluster_spark-master) \
/opt/spark/bin/spark-submit   --master spark://spark-master:7077 \
--class org.apache.spark.examples.SparkPi \
/opt/spark/examples/jars/spark-examples_2.13-4.2.0.jar 1000

docker exec -it $(docker ps -q -f name=spark-cluster_spark-master) \
/opt/spark/bin/spark-submit   --master spark://spark-master:7077 \
--class org.apache.spark.examples.JavaLogQuery \
/opt/spark/examples/jars/spark-examples_2.13-4.2.0.jar \
/opt/spark/data/access.log


docker exec -it $(docker ps -q -f name=spark-cluster_spark-master) \
/opt/spark/bin/spark-submit   --master spark://spark-master:7077 \
--class org.apache.spark.examples.JavaLogQuery \
/opt/spark/data/spark-examples_2.13-5.0.0-SNAPSHOT.jar \
/opt/spark/data/access.log 5000
