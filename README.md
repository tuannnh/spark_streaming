## Overview
Base on provided data (bookings.csv), use Faker library to generate the CSV data and send it continuously using Apache Kafka. Use  Spark Structured Streaming to read CSV files and consume the message sent from Kafka. Then load it within micro-batching to PostgreSQL as data mart, then visualize the data to get insights using Google Looker Studio
![[MCI (1).png]]
## Setup environment for developing
### Installations:
	1. Vagrant - Virtual machine tool like VMware or HyperV
	2. Ubuntu 22.04 - Operation System
	3. Docker
	4. Apache Kafka
	5. Apache Spark
	6. Python's libraries and dependencies

#### 1. Install Vagrant
```bash
	brew install hashicorp/tap/hashicorp-vagrant
```
- Navigate to a folder. In this project I use **MCI**. Create a **Vagrantfile** and a folder to store data that sync with the virtual server![[Pasted image 20230610235143.png]]

#### 2. Install Ubuntu 22.04
- Specify OS image to Ubuntu 22.04 and synced_folder for virtual machine and host machine ![[Pasted image 20230610235503.png]]
- Run the following command to start and access to the virtual the machine:
```bash
vagrant up
vagrant ssh
```
#### 3. Install Docker
```shell
# First, update existing list of packages
sudo apt update

# Next, install a few prerequisite packages which let `apt` use packages over HTTPS
sudo apt install apt-transport-https ca-certificates curl software-properties-common

# Then add the GPG key for the official Docker repository to system
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add the Docker repository to APT sources
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update existing list of packages again for the addition to be recognized
sudo apt update

# Make sure to install from the Docker repo instead of the default Ubuntu repo
apt-cache policy docker-ce

# Finally, install Docker
sudo apt install docker-ce

# addition, install docker compose
sudo apt install docker-compose
```
#### 4 & 5. Install Apache Kafka and Apache Spark
- Prepare a docker file
```yaml
version: '3'

services:

# ----------------- #
# Apache Spark #
# ----------------- #

	spark:
		image: docker.io/bitnami/spark:3.3
		container_name: spark-master
		environment:
		- SPARK_MODE=master
		ports:
		- '8080:8080'
		- '4040:4040'
		- '7077:7077'
		volumes:
		- ./data:/data
		- ./src:/src
	spark-worker:
		image: docker.io/bitnami/spark:3.3
		container_name: spark-worker
		environment:
		- SPARK_MODE=worker
		- SPARK_MASTER_URL=spark://spark:7077
		- SPARK_WORKER_MEMORY=4G
		- SPARK_EXECUTOR_MEMORY=4G
		- SPARK_WORKER_CORES=4
		volumes:
		- ./data:/data
		- ./src:/src
  
# ----------------- #
# Apache Kafka #
# ----------------- #

	zookeeper:
		image: docker.io/bitnami/zookeeper:3.8
		container_name: zookeeper
		ports:
		- "2181:2181"
		environment:
		- ALLOW_ANONYMOUS_LOGIN=yes
	kafka:
		image: docker.io/bitnami/kafka:3.3
		container_name: kafka
		ports:
		- "9092:9092"
		environment:
		- KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
		- ALLOW_PLAINTEXT_LISTENER=yes
		depends_on:
		- zookeeper
```
- Run command
```shell
sudo docker-compose up
```

#### 6. Python's libraries and dependencies
```shell
pip install faker psycopg2 pycountry
```
- **faker:** generate sample data for developing
- **psycopg2:** PostgreSQL connector in Python
- **pycountry:** Map country code to country name

## Implementation
- Create a "**bookings**" Kafka topic to receive our messages:
```shell
docker exec kafka kafka-topics.sh --create --replication-factor 1 --bootstrap-server localhost:9092 --topic bookings
```
-  Write function to generate data to act like system activity, then send it through Apache Kafka Producer
```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from faker import Faker
import csv
import random
import time
from datetime import datetime, timedelta

  

def generate_data():
	data = []
	# Initialize the Faker library
	fake = Faker()
	# Set the start and end dates for data generation
	start_date = datetime(2020, 1, 1)
	end_date = datetime(2023, 6, 10

	#Generate n=150 records
	for _ in range(150):
		created_date = fake.date_between_dates(date_start=start_date, date_end=end_date)
		arrival_date = (created_date + timedelta(days=fake.random_int(min=1, max=60)))
		departure_date = (arrival_date + timedelta(days=fake.random_int(min=2, max=7)))

		# List of possible status values
		status_values = ['O', 'N']
	
		# List of possible channel values
		channel_values = ['', 'online', 'offline', 'other']

		# Generate and format the data for each record
		guest_id = fake.random_int(min=10000, max=99999)
		status = fake.random_element(elements=status_values)
		room_group_id = fake.random_int(min=1, max=2)
		created_date = created_date.strftime('%Y-%m-%d')
		arrival_date = arrival_date.strftime('%Y-%m-%d')
		departure_date = departure_date.strftime('%Y-%m-%d')
		room_price = round(random.uniform(100, 300), 2)
		channel = fake.random_element(elements=channel_values)
		room_no = fake.random_int(min=100, max=999)
		country = fake.country_code()
		adults = fake.random_int(min=0, max=3)
		children = fake.random_int(min=0, max=2)
		total_payment = round(room_price * (adults + children), 2)
		
		
		record = [
		guest_id, status, room_group_id, created_date, arrival_date, departure_date,
		room_price, channel, room_no, country, adults, children, total_payment
		]
		  
		data.append(record)
					
	file_name = '/data/generated_data.csv'
	with open(file_name, 'w', newline='') as file:
	writer = csv.writer(file)
	writer.writerow(['GuestID', 'Status', 'RoomGroupID', 'CreatedDate', 'ArrivalDate', 'DepartureDate', 'RoomPrice', 'Channel', 'RoomNo', 'Country', 'Adults', 'Children', 'TotalPayment'])
	writer.writerows(data)
						
	print('csv file generated successfully!')

  

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "bookings"
FILE_PATH = "/data/"


spark = SparkSession.builder.appName("write_bookings_topic").getOrCreate()
spark.sparkContext.setLogLevel("WARN") # Reduce logging verbosity
  
generate_data()

df_booking_stream = spark.read\
.format("csv") \
.option("header", "true") \
.load(FILE_PATH)\
.na.fill({"Channel": "unknown"})\
.withColumn("value", F.to_json( F.struct(F.col("*")) ) )\
.withColumn("key", F.lit("key"))\

  
#Write one row at a time to the topic
for row in df_booking_stream.collect():
					
	# transform row to dataframe
	df_row = spark.createDataFrame([row.asDict()])
	
	# write a row to topic every 2 second
	df_row.write\
	.format("kafka")\
	.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
	.option("topic", KAFKA_TOPIC)\
	.save()
	
	print(f"Row written to topic {KAFKA_TOPIC}")
	time.sleep(2)
```
- Name the file **spark_producer.py** and put it in **/src/streaming** directory. Then execute the job:
```shell
docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/spark_producer.py
```
- Moving on, let’s create a job to consume the data, have a simple transformation, let's have a file named **spark_consumer.py** and put it in **/src/streaming** directory too. 
```python
from pyspark.sql import SparkSession  
import pyspark.sql.functions as F  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType  
import pycountry  
import psycopg2  
  
# Function to map country code to country name for humanreadable  
def map_country_code_to_name(country_code):  
    try:  
        country = pycountry.countries.get(alpha_2=country_code)  
        if country:  
            return country.name  
        else:  
            return None  
    except Exception as e:  
        print("Error occurred:", e)  
        return None  

# Have to create user defined funtion to transform the data
map_country_code_UDF = F.udf(lambda x:map_country_code_to_name(x),StringType())   
  
spark = SparkSession.builder\  
.appName('bookings_consumer')\  
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1')\  
.getOrCreate()  
  
# Reduce logging verbosity  
spark.sparkContext.setLogLevel("WARN")  
  
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  
KAFKA_TOPIC = "bookings"  
SCHEMA = StructType([  
    StructField("GuestID", StringType()),  
    StructField("Status", StringType()),  
    StructField("RoomGroupID", StringType()),  
    StructField("CreatedDate", StringType()),  
    StructField("ArrivalDate", StringType()),  
    StructField("DepartureDate", StringType()),  
    StructField("RoomPrice", StringType()),  
    StructField("Channel", StringType()),  
    StructField("RoomNo", StringType()),  
    StructField("Country", StringType()),  
    StructField("Adults", StringType()),  
    StructField("Children", StringType()),  
    StructField("TotalPayment", StringType())  
])  
  
spark.sparkContext.setLogLevel("WARN")  
  
  
df_booking_stream = spark\  
    .readStream.format("kafka")\  
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\  
    .option("subscribe", KAFKA_TOPIC)\  
    .load()\  
    .select(  
    F.from_json(  
        # decode string as iso-8859-1  
        F.decode(F.col("value"), "iso-8859-1"),  
        SCHEMA    ).alias("value")  
)\  
    .select("value.*")\  
    .withColumn("CountryName", map_country_code_UDF(F.col("Country")))\  
      
# PostgreSQL connection info  
postgres_properties = {  
    "host": "remote.hungtuan.me",  
    "port": "5432",  
    "database": "mci",  
    "user": "tuan",  
    "password": ""  
}  
  
# Define the function to write each micro-batch to PostgreSQL  
def write_to_postgres(df, epoch_id):  
    print("Writing micro-batch to PostgreSQL")  
    rows = df.collect()  
  
    # Establish a connection to PostgreSQL  
    conn = psycopg2.connect(**postgres_properties)  
    cur = conn.cursor()  
  
    for row in rows:  
        print(row)  
        insert_query = f'''  
            INSERT INTO bookings ("guest_id", "status", "room_group_id", "created_date", "arrival_date", "departure_date", "room_price", "channel", "room_no",            "country", "adults", "children", "total_payment", "country_name")   
            VALUES ({row.GuestID}, "{row.Status}", "{row.RoomGroupID}", {row.CreatedDate}, {row.ArrivalDate}, {row.DepartureDate}, {row.RoomPrice}, "{row.Channel}", {row.RoomNo},   
            "{row.Country}", {row.Adults}, {row.Children}, {row.TotalPayment}, "{row.CountryName}");  
            '''  
        cur.execute('INSERT INTO bookings ("guest_id", "status", "room_group_id", "created_date", "arrival_date",\  
         "departure_date", "room_price", "channel", "room_no", "country", "adults", "children", "total_payment", "country_name") \         VALUES (%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (row.GuestID, row.Status, row.RoomGroupID, row.CreatedDate, row.ArrivalDate, row.DepartureDate, row.RoomPrice, row.Channel, row.RoomNo, row.Country, row.Adults, row.Children, row.TotalPayment, row.CountryName))  
  
    print("Write completed successfully")  
  
    # Commit the changes and close the connection  
    conn.commit()  
    cur.close()  
    conn.close()  
  
# Apply the foreachBatch operation to the streaming DataFrame  
query = df_booking_stream.writeStream \  
    .foreachBatch(write_to_postgres) \  
    .start()  
  
# Wait for the streaming query to finish  
query.awaitTermination()
```
- The data now is ready in our data mart. It's reporting time! 
- Create a new report in Looker Studio. Set up connection to our PostgreSQL
![[Pasted image 20230611133305.png]]
- Put some charts and visualizations to understand out data.
![[Pasted image 20230612112734.png]]
- Link to live dashboard: https://lookerstudio.google.com/reporting/d6d8f6d2-6e5f-4d2d-9d12-a190f9a84f0c
