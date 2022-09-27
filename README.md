# Big Data Technology Project Sep-2022
CS523DE-MIU  
#KARIM_ABURJEILA  


This project to analyze Covid19 data until Feb-2020 using Kafka, spark, hive It's a fake data   

Requirements
Starting point should be Cloudera Quick Start machine.
https://www.cloudera.com/downloads/quickstart_vms/5-13.html

Java 1.8 .
Apache Nifi : 
Login as a root : su
Create nifi folder
mkdir /usr/bin/nifi
cd /usr/bin/nifi


#Kafka installation : 
Install Kafka and kafka-server using yum from the cloudera packages
sudo yum clean all
sudo yum install kafka
sudo yum install kafka-server
To start Kafka service use this command
sudo service kafka-server start
 

# Create a topic on Kafka
./Kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic COVID19_LINES
./Kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic COVID19_LINES


./bin/kafka-topics.sh --list --zookeeper quickstart.cloudera:2181
 
#-- permission /tmp/hive
 sudo chmod -R 777 /tmp/hive
 
# update Spark 2.x
https://medium.com/@yesilliali/installing-apache-spark-2-x-to-cloudera-quickstart-wm-dd5314e6d9bd


# git hub clone COVID-19
sudo git clone https://github.com/CSSEGISandData/COVID-19.git
git pull origin https://github.com/CSSEGISandData/COVID-19.git


#How to run:

kafka-console-producer  --broker-list localhost:9092 --topic COVID19_LINES
kafka-console-consumer --zookeeper localhost:2181 --topic COVID19_LINES --from-beginning

-----or 
-- dir /home/cloudera/Desktop/zookeeper/bin
 sudo ./zookeeper/bin/zkServer.sh start

-- dir  /home/cloudera/Desktop/Kafka/bin
sudo ./kafka-server-start.sh ../config/server.properties 



##Project Archtiture

[![Project Archtiture](https://github.com/KareemAbuRejila/dbt-project/blob/eaba986e1613070973e888c8c96d259490cf0c5d/images/archit.png)


## Chart deaths by Country 1

[![Phart deaths by Country](https://github.com/KareemAbuRejila/dbt-project/blob/eaba986e1613070973e888c8c96d259490cf0c5d/images/bycountry.pdf)


## Chart deaths by Country 2

[![Phart deaths by Country](https://github.com/KareemAbuRejila/dbt-project/blob/eaba986e1613070973e888c8c96d259490cf0c5d/images/deaths.pdfg)

