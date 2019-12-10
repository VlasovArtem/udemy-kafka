# Udemy

Course link - https://www.udemy.com/course/apache-kafka

# Use Cases

* Messaging System
* Activity Tracking
* Gather metrics from many different locations
* Applications logs gathering
* Stream processing (with Kafka Streams API or Spark for example)
* De-coupling of system dependencies
* Integration with Spark, Flink, Storm, Hadoop, and many other Big Data technologies

# Topics, partitions and offsets

## Topics

A particular stream of data

* Similar to a table in a database (without all the constraints)
* You can have as many topics as you want
* A topic is identified by its name

Topics are split in <u>partitions</u>

## Partitions

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

**<u>At any time only ONE broker can be a leader for a given partition</u>**

<u>**Only that leader can receive and serve data for a partition**</u>

The order brokers will synchronise the data

Therefore each partition has one leader and multiple ISR (in-sync replica)

# Producers

Producers write data to topics (which is made of partitions)

Producers automatically know to which broker and partition to write to

In case of Broker failures, Producers will automatically recover

Producers can choose to receive acknowledgment of data writes:

* <u>acks=0</u> - Producer won't wait for acknowledgment (possible data loss)
* acks=1 - Producer will wait for leader acknowledgment (limited data loss)
* acks=all - Leader + replicas acknowledgment (no data loss)

## Message keys

Producers can choose to send a key with the message. Message can be string, number, etc.

If key=null, data is sent round robin (first broker then second broker then third broker... )

If a key is sent, then all messages for that key will always go to the same partition

A key is basically sent if you need message ordering for a specific field. This guarantee thanks to key hashing, which depends on the number of partitions.

# Consumers

Consumers read data from a topic (identified by name)

Consumers know which broker to read from

In case of broker failures, consumers know how to recover

Data is read in order <u>**within each partitions**</u>

## Consumer Groups

Consumers read data in consumer groups

Each consumer within a group reads from exclusive partitions

If you have more consumers than partitions, some consumers will be inactive

*Note: Consumers will automatically use a GroupCoordinator and a ConsumerCoordinator to assign a consumers to a partition.*

## Consumer Offsets

Kafka stores the offsets at which a consumer group has been reading

The offsets committed live in a Kafka topic named **__consumer_offsets**

When a consumer in a group has processed data received from Kafka, it should be committing the offsets

If  a consumer dies, it will be able to read back from where it left off thanks to the committed consumer offsets!

### Delivery semantics for consumers

Consumer choose when to commit offsets 

There are 3 delivery semantics:

1. At most once:
   * offsets are committed as soon as the message is received
   * if the processing goes wrong, the message will be lost (it won't be read again)
2. At least once (usually preferred):
   * offsets are committed after the message is processed
   * if the processing goes wrong, the message will be read again
   * this can result in duplicate processing of messages. Make sure your processing is <u>idempotent</u> (i.e. processing again the messages won't impact your system)
3. Exactly once:
   * Can achieved for Kafka => Kafka workflows using Kafka Streams API
   * For Kafka => External System workflows, use a <u>idempotent</u> consumer

# Kafka Broker Discovery

Every Kafka broker is also called a "bootstrap server"

That means that **you only need to connect to one broker**, and you will be connected to the entire cluster

Each broker knows about all brokers, topics and partitions (metadata)

# Zookeeper

Zookeeper manages brokers (keeps a list of them)

Zookeeper helps in performing leader election for partition

Zookeeper sends notifications to Kafka in case of changes (e.g. new topic, broker dies, broker comes up, delete topics, etc...)

**Kafka can't work without Zookeeper**

Zookeeper by design operates with an odd number of servers (3, 5, 7)

Zookeeper has a leader (handle writes) the rest of the servers are followers (handle reads)

Zookeeper does NOT store consumer offsets with Kafka > v0.10

# Kafka Guarantees

1. Messages are appended to a topic-partition in the order they are sent
2. Consumers read messages in the order stored in a topic-partition
3. With a replication factor of N, producers and consumers can tolerate up to N-1 brokers being down
4. This is why a replication factor of 3 is a good idea
   * Allows for one broker to be taken down for maintenance
   * Allows for another broker to be taken down unexpectedly
5. As long as the number of partitions remains constant for a topic (no new partitions), the same key will always go to the same partition