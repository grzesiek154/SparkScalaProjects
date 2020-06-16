*The following post should serve as a guide for those trying to understand  of inner-workings of Apache Spark. I have created it initially for  organizing my knowledge and extended later on. It assumes that you,  however, possess some basic knowledge of Spark.*

*All examples are written in Python 2.7 running with PySpark 2.1 but the rules are very similar for other APIs*.

------

First of some words about the most basic concept — a partition:

> **Partition** — a logical *chunk* of a large data set.

Very often data we are processing can be separated into logical partitions  (ie. payments from the same country, ads displayed for given cookie,  etc). In Spark, they are distributed **among nodes** when *shuffling* occurs.

> *Spark can run 1 concurrent task for every partition of an RDD (up to the  number of cores in the cluster). If you’re cluster has 20 cores, you  should have at least 20 partitions (in practice* [*2–3x times more*](http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism)*). From the other hand a single partition typically shouldn’t contain more than 128MB and a single shuffle block cannot be larger than 2GB (see* [*SPARK-6235*](https://issues.apache.org/jira/browse/SPARK-6235)*).*

In general, *more numerous* partitions allow work to be distributed among more workers, but *fewer partitions* allow work to be done in larger chunks (and often quicker).

> *Spark’s partitioning feature is available on all* ***RDDs of key/value pairs\****.*

# Why care?

For one, quite important reason — *performance*. By having all relevant data in one place (node) we reduce the overhead  of shuffling (need for serialization and network traffic).

Also understanding how Spark deals with partitions allow us to control the  application parallelism (which leads to better cluster utilization —  fewer costs).

But keep in mind that partitioning will not be helpful in all applications. For example, if a given RDD is scanned only once, there is no point in  partitioning it in advance. It’s useful only when a dataset is reused *multiple times* (in key-oriented situations using functions like `join()`).

We will use the following list of numbers for investigating the behavior.

```python
from pyspark import SparkContext
# The default data used for calculations
nums = range(0, 10)
print(nums)
```

Output

```
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

# Playing with partitions

Let’s start with creating a local context with allocated one thread only and  parallelizing a collection with using all defaults. We are going to use `glom()`function that will expose the structure of created partitions.

> ***From API\****:* `*glom()*`*-* return an RDD created by coalescing all elements within each partition into a list.

Each RDD also possesses information about partitioning schema (you will see  later that it can be invoked explicitly or derived via some  transformations).

> ***From API\****:* `*partitioner*` *-* inspect partitioner information used for the RDD*.*

```python
with SparkContext("local") as sc:
    rdd = sc.parallelize(nums)
    
    print("Number of partitions: {}".format(rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(rdd.glom().collect()))
```

Output

```python
Number of partitions: 1
Partitioner: None
Partitions structure: [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]
```

Ok, so what happened under the hood?

Spark uses different partitioning schemes for various types of RDDs and operations. In a case of using `parallelize()` data is evenly distributed between partitions using their indices (no partitioning scheme is used).

> *If there is* ***no partitioner\*** *the partitioning is not based upon characteristic of data but distribution is random and uniformed across nodes.*
>
> *Different rules apply for various data sources and structures (ie. when loading data using* `*textFile()*` *or using tuple objects). Good sumary is provided* [*here*](https://techmagie.wordpress.com/2015/12/19/understanding-spark-partitioning/)*.*

If you look inside `parallelize()` source code you will see that the number of partitions can be distinguished either by setting `numSlice` argument or by using `spark.defaultParallelism` property (which is reading context information).

Now let’s try to allow our driver to use two local cores.

```python
with SparkContext("local[2]") as sc:
    rdd = sc.parallelize(nums)
    
    print("Default parallelism: {}".format(sc.defaultParallelism))
    print("Number of partitions: {}".format(rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(rdd.glom().collect()))
```

Output

```
Default parallelism: 2
Number of partitions: 2
Partitioner: None
Partitions structure: [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```

Ok, that worked as expected — the data was distributed across two partitions and each will be executed in a separate thread.

But what will happen when the number of partitions exceeds the number of data records?

```python
with SparkContext("local") as sc:
    rdd = sc.parallelize(nums, 15)
    
    print("Number of partitions: {}".format(rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(rdd.glom().collect()))
```

Output

```
Number of partitions: 15
Partitioner: None
Partitions structure: [[], [0], [1], [], [2], [3], [], [4], [5], [], [6], [7], [], [8], [9]]
```

You can see that Spark created requested a number of partitions but most of them are empty. This is bad because the time needed to prepare a new  thread for processing data (one element) is significantly greater than  processing time itself (you can analyze it in Spark UI).

# Custom partitions with `partitionBy()`

`partitionBy()` transformation allows applying custom partitioning logic over the RDD.

Let’s try to partition the data further by taking advantage of domain-specific knowledge.

> ***Warning\*** *— to use* `*partitionBy()*` *RDD must consist of tuple (pair) objects. It's a* transformation*, so a new RDD will be returned. It's highly adviseable to persist it for more optimal later usage.*

Because `partitionBy()` requires data to be in key/value format we will need to transform the data.

> *In PySpark an object is considered valid for* `*PairRDD*` *operations if it can be unpacked as follows* `*k, v = kv*`*. You can read more about the requirements* [*here*](http://stackoverflow.com/questions/35703298/how-to-determine-if-object-is-a-valid-key-value-pair-in-pyspark)*.*

```python
with SparkContext("local[2]") as sc:
    rdd = sc.parallelize(nums) \
        .map(lambda el: (el, el)) \
        .partitionBy(2) \
        .persist()
    
    print("Number of partitions: {}".format(rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(rdd.glom().collect()))
```

Output

```
Number of partitions: 2
Partitioner: <pyspark.rdd.Partitioner object at 0x7f97a56fabd0>
Partitions structure: [[(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)], [(1, 1), (3, 3), (5, 5), (7, 7), (9, 9)]]
```

You can see that now the elements are distributed differently. A few interesting things happened:

1. `parallelize(nums)` - we are transforming Python array into RDD with no partitioning scheme,
2. `map(lambda el: (el, el))` - transforming data into the form of a *tuple*,
3. `partitionBy(2)` - splitting data into 2 chunks using default *hash partitioner*,

Spark used a *partitioner function* to distinguish which to which partition assign each record. It can be specified as the second argument to the `partitionBy()`. The partition number is then evaluated as follows `partition = partitionFunc(key) % num_partitions`.

> *By default PySpark implementation uses* hash partitioning *as the partitioning function.*

Let’s perform an additional sanity check.

```python
from pyspark.rdd import portable_hash
num_partitions = 2
for el in nums:
    print("Element: [{}]: {} % {} = partition {}".format(
        el, portable_hash(el), num_partitions, portable_hash(el) % num_partitions))
```

Output

```
Element: [0]: 0 % 2 = partition 0
Element: [1]: 1 % 2 = partition 1
Element: [2]: 2 % 2 = partition 0
Element: [3]: 3 % 2 = partition 1
Element: [4]: 4 % 2 = partition 0
Element: [5]: 5 % 2 = partition 1
Element: [6]: 6 % 2 = partition 0
Element: [7]: 7 % 2 = partition 1
Element: [8]: 8 % 2 = partition 0
Element: [9]: 9 % 2 = partition 1
```

But let’s get into a more realistic example. Imagine that our data consist  of various dummy transactions made across different countries.

```python
transactions = [
    {'name': 'Bob', 'amount': 100, 'country': 'United Kingdom'},
    {'name': 'James', 'amount': 15, 'country': 'United Kingdom'},
    {'name': 'Marek', 'amount': 51, 'country': 'Poland'},
    {'name': 'Johannes', 'amount': 200, 'country': 'Germany'},
    {'name': 'Paul', 'amount': 75, 'country': 'Poland'},
]
```

We know that further analysis will be performed analyzing many similar  records within the same country. To optimize network traffic it seems to be a good idea to put records from one country in one node.

To meet this requirement, we will need a *custom partitioner*:

> ***Custom partitioner\*** *— function returning an integer for given object (tuple key).*

```python
# Dummy implementation assuring that data for each country is in one partition
def country_partitioner(country):
    return hash(country)
# Validate results
num_partitions = 5
print(country_partitioner("Poland") % num_partitions)
print(country_partitioner("Germany") % num_partitions)
print(country_partitioner("United Kingdom") % num_partitions)
```

Output

```
1
1
4
```

By validating our partitioner we can see what partitions are assigned for each country.

> *Pay attention for potential data skews. If some keys are overrepresented in the dataset it can result in suboptimal resource usage and potential  failure.*

```python
with SparkContext("local[2]") as sc:
    rdd = sc.parallelize(transactions) \
        .map(lambda el: (el['country'], el)) \
        .partitionBy(4, country_partitioner)
    
    print("Number of partitions: {}".format(rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(rdd.glom().collect()))
```

Output

```
Number of partitions: 4
Partitioner: <pyspark.rdd.Partitioner object at 0x7f97a56b7bd0>
Partitions structure: [[('United Kingdom', {'country': 'United Kingdom', 'amount': 100, 'name': 'Bob'}), ('United Kingdom', {'country': 'United Kingdom', 'amount': 15, 'name': 'James'}), ('Germany', {'country': 'Germany', 'amount': 200, 'name': 'Johannes'})], [], [('Poland', {'country': 'Poland', 'amount': 51, 'name': 'Marek'}), ('Poland', {'country': 'Poland', 'amount': 75, 'name': 'Paul'})], []]
```

It worked as expected all records from a single country is within one  partition. We can do some work directly on them without worrying about  shuffling by using the `mapPartitions()` function.

> ***From API\****:* `*mapPartitions()*` *converts each* partition *of the source RDD into multiple elements of the result (possibly none). One important usage can be some* ***heavyweight initialization\*** *(that should be done once for many elements). Using* `*mapPartitions()*` *it can be done once per worker task/thread/partition instead of running* `*map()*` *for each RDD data element.*

In the example below, we will calculate the sum of sales in each partition (in this case such operations make no sense, but the point is to show  how to pass data into `mapPartitions()` function).

```python
# Function for calculating sum of sales for each partition
# Notice that we are getting an iterator.All work is done on one node
def sum_sales(iterator):
    yield sum(transaction[1]['amount'] for transaction in iterator)
with SparkContext("local[2]") as sc:
    by_country = sc.parallelize(transactions) \
        .map(lambda el: (el['country'], el)) \
        .partitionBy(3, country_partitioner)
    
    print("Partitions structure: {}".format(by_country.glom().collect()))
    
    # Sum sales in each partition
    sum_amounts = by_country \
        .mapPartitions(sum_sales) \
        .collect()
    
    print("Total sales for each partition: {}".format(sum_amounts))
```

Output

```
Partitions structure: [[('Poland', {'country': 'Poland', 'amount': 51, 'name': 'Marek'}), ('Germany', {'country': 'Germany', 'amount': 200, 'name': 'Johannes'}), ('Poland', {'country': 'Poland', 'amount': 75, 'name': 'Paul'})], [('United Kingdom', {'country': 'United Kingdom', 'amount': 100, 'name': 'Bob'}), ('United Kingdom', {'country': 'United Kingdom', 'amount': 15, 'name': 'James'})], []]
Total sales for each partition: [326, 115, 0]
```

# Working with DataFrames

Nowadays we are all advised to abandon operations on raw RDDs and use structured DataFrames (or Datasets if using Java or Scala) from Spark SQL module.  Creators made it very easy to create *custom partitioners* in this case.

```python
from pyspark.sql import SparkSession, Row
with SparkSession.builder \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", 50) \
        .getOrCreate() as spark:
    
    rdd = spark.sparkContext \
        .parallelize(transactions) \
        .map(lambda x: Row(**x))
    
    df = spark.createDataFrame(rdd)
    
    print("Number of partitions: {}".format(df.rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(df.rdd.glom().collect()))
    
    # Repartition by column
    df2 = df.repartition("country")
    
    print("\nAfter 'repartition()'")
    print("Number of partitions: {}".format(df2.rdd.getNumPartitions()))
    print("Partitioner: {}".format(df2.rdd.partitioner))
    print("Partitions structure: {}".format(df2.rdd.glom().collect()))
```

Output

```
Number of partitions: 2
Partitioner: None
Partitions structure: [[Row(amount=100, country=u'United Kingdom', name=u'Bob'), Row(amount=15, country=u'United Kingdom', name=u'James')], [Row(amount=51, country=u'Poland', name=u'Marek'), Row(amount=200, country=u'Germany', name=u'Johannes'), Row(amount=75, country=u'Poland', name=u'Paul')]]After 'repartition()'
Number of partitions: 50
Partitioner: None
Partitions structure: [[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [Row(amount=200, country=u'Germany', name=u'Johannes')], [], [Row(amount=51, country=u'Poland', name=u'Marek'), Row(amount=75, country=u'Poland', name=u'Paul')], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [Row(amount=100, country=u'United Kingdom', name=u'Bob'), Row(amount=15, country=u'United Kingdom', name=u'James')], [], [], [], []]
```

You can see that DataFrames expose a modified `repartition()` method taking as an argument a column name. When not specifying number  of partitions a default value is used (taken from the config parameter `spark.sql.shuffle.partitions`).

Let’s take a closer look at this method at the general.

# `coalesce()` and `repartition()`

`coalesce()` and `repartition()` transformations are used for **changing the number of partitions** in the RDD.

> `*repartition()*` *is calling* `*coalesce()*` *with explicit shuffling.*

The rules for using are as follows:

- if you are **increasing** the number of partitions use `repartition()`(*performing full shuffle*),
- if you are **decreasing** the number of partitions use `coalesce()` (*minimizes shuffles*)

Code below shows how repartitioning works (data is represented using DataFrames).

```python
with SparkSession.builder \
        .master("local[2]") \
        .getOrCreate() as spark:
    
    nums_rdd = spark.sparkContext \
        .parallelize(nums) \
        .map(lambda x: Row(x))
    
    nums_df = spark.createDataFrame(nums_rdd, ['num'])
    
    print("Number of partitions: {}".format(nums_df.rdd.getNumPartitions()))
    print("Partitions structure: {}".format(nums_df.rdd.glom().collect()))
    
    nums_df = nums_df.repartition(4)
    
    print("Number of partitions: {}".format(nums_df.rdd.getNumPartitions()))
    print("Partitions structure: {}".format(nums_df.rdd.glom().collect()))
```

Output

```
Number of partitions: 2
Partitions structure: [[Row(num=0), Row(num=1), Row(num=2), Row(num=3), Row(num=4)], [Row(num=5), Row(num=6), Row(num=7), Row(num=8), Row(num=9)]]Number of partitions: 4
Partitions structure: [[Row(num=1), Row(num=6)], [Row(num=2), Row(num=7)], [Row(num=3), Row(num=8)], [Row(num=0), Row(num=4), Row(num=5), Row(num=9)]]
```

# Vanishing partitioning schema

Many available RDD operations will take advantage of underlying partitioning. On the other hand operations like `map()` cause the new RDD to *forget* the parent's partitioning information.

## Operations that benefit from partitioning

All operations performing *shuffling data by key* will benefit from partitioning. Some examples are `cogroup()`, `groupWith()`, `join()`, `leftOuterJoin()`, `rightOuterJoin()`, `groupByKey()`, `reduceByKey()`, `combineByKey()` or `lookup()`.

## Operations that affect partitioning

Spark knows internally how each of it’s operations affects partitioning, and automatically sets the `partitioner` on RDDs created by operations that partition that data.

But the are some transformations that *cannot* guarantee to produce known partitioning — for example calling `map()` could theoretically modify the key of each element.

```python
with SparkContext("local[2]") as sc:
    rdd = sc.parallelize(nums) \
        .map(lambda el: (el, el)) \
        .partitionBy(2) \
        .persist()
    
    print("Number of partitions: {}".format(rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(rdd.glom().collect()))
    
    # Transform with `map()` 
    rdd2 = rdd.map(lambda el: (el[0], el[0]*2))
    
    print("Number of partitions: {}".format(rdd2.getNumPartitions()))
    print("Partitioner: {}".format(rdd2.partitioner))  # We have lost a partitioner
    print("Partitions structure: {}".format(rdd2.glom().collect()))
```

Output

```
Number of partitions: 2
Partitioner: <pyspark.rdd.Partitioner object at 0x7f97a5711310>
Partitions structure: [[(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)], [(1, 1), (3, 3), (5, 5), (7, 7), (9, 9)]]Number of partitions: 2
Partitioner: None
Partitions structure: [[(0, 0), (2, 4), (4, 8), (6, 12), (8, 16)], [(1, 2), (3, 6), (5, 10), (7, 14), (9, 18)]]
```

> *Spark does not analyze your functions to check whether they retain the key.*

Instead, there are some functions provided that guarantee that each tuple’s key remains the same — `mapValues()`, `flatMapValues()` or `filter()` (if the parent has a partitioner).

```python
with SparkContext("local[2]") as sc:
    rdd = sc.parallelize(nums) \
        .map(lambda el: (el, el)) \
        .partitionBy(2) \
        .persist()
    
    print("Number of partitions: {}".format(rdd.getNumPartitions()))
    print("Partitioner: {}".format(rdd.partitioner))
    print("Partitions structure: {}".format(rdd.glom().collect()))
    
    # Use `mapValues()` instead of `map()` 
    rdd2 = rdd.mapValues(lambda x: x * 2)
    
    print("Number of partitions: {}".format(rdd2.getNumPartitions()))
    print("Partitioner: {}".format(rdd2.partitioner))  # We still got partitioner
    print("Partitions structure: {}".format(rdd2.glom().collect()))
```

Output

```
Number of partitions: 2
Partitioner: <pyspark.rdd.Partitioner object at 0x7f97a56b7d90>
Partitions structure: [[(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)], [(1, 1), (3, 3), (5, 5), (7, 7), (9, 9)]]Number of partitions: 2
Partitioner: <pyspark.rdd.Partitioner object at 0x7f97a56b7d90>
Partitions structure: [[(0, 0), (2, 4), (4, 8), (6, 12), (8, 16)], [(1, 2), (3, 6), (5, 10), (7, 14), (9, 18)]]
```

# Memory issues

Have you ever seen this mysterious piece of text — `**java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE**`?

![img](https://miro.medium.com/max/30/1*hdn5r0Cg5JNLLIaKXXdldg.jpeg?q=20)

![img](https://miro.medium.com/max/568/1*hdn5r0Cg5JNLLIaKXXdldg.jpeg)

Looking into stack trace it can be spotted that it’s not coming from within you app but from Spark internals. The reason is that in Spark **you cannot have shuffle block greater than 2GB**.

> ***Shuffle block\*** *— data transferred across stages between executors.*

This happens because Spark uses `ByteBuffer` as abstraction for storing block and it's limited by `Integer.MAX_SIZE` (2 GB).

It’s especially problematic for Spark SQL (various aggregation functions)  because the default number of partitions to use when doing shuffle is  set to 200 (it can lead to high shuffle block sizes that can sometimes  exceed 2GB).

So what can be done:

1. Increase the number of partitions (thereby, reducing the average partition size) by increasing the value of `spark.sql.shuffle.partitions` for Spark SQL or by calling `repartition()` or `coalesce()` on RDDs,
2. Get rid of skew in data

> *It’s good to know that Spark uses* [*different logic for memory management when the number of partitions is greater than 2000*](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala#L48) *(uses high compression algorithm). So if you have ~2000 partitions it’s worth bumping it up to 2001 which will result in smaller memory  footprint.*

# Take-aways

Spark partitioning is available on all RDDs of key/value pairs and causes the system to group elements based on a function of each key.

# Features

- tuples in the same partition are guaranteed to be on the same machine,
- each node in the cluster can contain more than one partition,
- the total number of partitions are configurable (by default set to the total number of cores on all executor nodes)

# Performance tuning checklist

- have the correct number of partitions (according to cluster specification) — check [this](https://techmagie.wordpress.com/2015/12/19/understanding-spark-partitioning/) and [that](http://stackoverflow.com/documentation/apache-spark/5822/partitions#t=201609112000500779093) for guidance,
- consider using custom partitioners,
- check if your transformations preserve partition schema,
- check if memory could be optimized by bumping number of partitions to 2001

# Settings

- `spark.default.parallelism` - sets up the number of partitions to use for HashPartitioner (can be overridden when creating `SparkContext` object),
- `spark.sql.shuffle.partitions` - controls the number of partitions for operations on `DataFrames` (default is 200)

As the final thought note that the number of partitions also determine how many files will be generated by actions saving an RDD to files.

# Sources

- [“Partitions and Partitioning” by Jacek Laskowski](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-partitions.html)
- [Default Partitioning Scheme in Spark](http://stackoverflow.com/questions/34491219/default-partitioning-scheme-in-spark)
- [“Learning Spark” by Holden Karau](https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch04.html)
- [Understanding Spark Partitioning](https://techmagie.wordpress.com/2015/12/19/understanding-spark-partitioning/)
- [Controlling Parallelism in Spark](http://www.bigsynapse.com/spark-input-output)
- [“Managing Spark Partitions with Coalesce and Repartition” by Matthew Powers](https://hackernoon.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4)
- [Top 5 Mistakes When Writing Spark Applications](https://www.youtube.com/watch?v=WyfHUNnMutg)