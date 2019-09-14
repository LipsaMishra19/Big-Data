# Data-Intensive Distributed Computing - CS 651

## Assignment 0: 

#### Linux Student CS environment

### Question 1: In the Shakespeare collection, what is the most frequent x and how many times does it appear?

Answer: hadoop fs -cat cs451-bigdatateach-a0-shakespeare/part-r-00000 | sort -k 2 -n -r | head -1 

Output: Love 4 

#### Using the Datasci Cluster

### Question 2: Run word count on the Datasci cluster and make sure you can access the Resource Manager webapp. What is your application id?

Answer : Application ID for Word Count: application_1566180955559_0063
         Application ID for PerfectX: application_1566180955559_0065

### Question 3: For this word count job, how many mappers ran in parallel?

Answer : 14

### Question 4: From the word count program, how many times does "waterloo" appear in the sample Wikipedia collection?

Answer : hadoop fs -cat wc-jmr-combiner/part* | grep -E '^waterloo(\s)+'

Output: 3620

### Question 5: In the sample Wikipedia collection, what are the 10 most frequent x's and how many times does each appear?

Answer 1: hadoop fs -cat cs451-bigdatateach-a0-wiki/part* | sort -k 2 -n -r | head -10

Output:
game	369
for	251
day	187
and	176
score	151
world	137
record	133
strangers	123
the	116
season	113

OR:

Answer 2: hadoop fs -cat cs451-bigdatateach-a0-wiki/part* | sort -k 2 -n | tail -10

Output: 
season	113
the	116
strangers	123
record	133
world	137
score	151
and	176
day	187
for	251
game	369
