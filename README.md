# Use Cases

* Messaging System
* Activity Tracking
* Gather metrics from many different locations
* Applications logs gathering
* Stream processing (with Kafka Streams API or Spark for example)
* De-coupling of system dependencies
* Integration with Spark, Flink, Storm, Hadoop, and many other Big Data technologies

# Topics, partitions and offsets

**Topics**

A particular stream of data

* Similar to a table in a database (without all the constraints)
* You can have as many topics as you want
* A topic is identified by its name

Topics are split in <u>partitions</u>

**Partitions**

Each partitions is ordered

Each message within a partition gets an incremental id, called <u>offset</u>

Number of partitions are set during creation of a topic

Offset only have a meaning for a specific partition (offset 3 in partition 0 doesn't repserent the same data as offset 3 in partition 1)

Order is guaranteed only within a partition (not across partitions)

Data is kept only for a limited time (default is one week)

One the data is written to a partition, i can't be changed (immutability)

Data is assigned randomly to a partition unless a key is provided 

# Brokers

A Kafka cluster is composed of multple brokers (servers)

Each broker is identified with its ID

Each broker contains certain topic partitions

After connecting to any broker (called a bootstrap broker), you will be connected to the entire cluster

A good number to get started is 3 brokers, but some big clusters have over 100 brokers

In these example we choose to number brokers starting at 100 (arbitrary)

# Topic replication factor

Topics should have a replication factor > 1 (usually between 2 and 3 - 3 is gold standart)

This way if a broker is down, another broker can serve the data



