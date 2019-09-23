# ASSIGNMENT 1: Counting in MapReduce
## Question 1. Briefly describe in prose your solution, both the pairs and stripes implementation. 

### Pairs Implementation: The pairs implementation uses two MapReduce jobs. In the First MapReduce job, the first mapper is used to count every word in each line and emits unique words (WORD, 1). It also tracks the count of the number of lines using a Counter.  The first reducer then sums up all the values that it received from the first mapper and emits output as (WORD,SUM). The second mapper takes shakespeare data file as input and counts every pair of words that occur in the file , except the self pair and emits output as ((WordX,WordY),1). It uses PairOfStrings to fetch (WordX,WordY). The combiner sums up all the counts of the pair of words calculated in the second mapper job and emits the output as ((WordX,WordY),SUM)). The second reducer counts the total number of lines, fetches the data (Word counts) from the first MapReduce job and then calculates the PMI using the formula and emits output as ((PairOfStrings (WordX, WordY), PairOfFloatInt (PMI, count)). The key is the co-occurence pair PairOfStrings (WordX, WordY) and value is a pair of PMI and co-occurence count PairOfFloatInt (PMI, count).



### Stripes Implementation: The stripes implementation uses two MapReduce jobs. In the First MapReduce job, the first mapper is used to count every word in each line and emits unique words (WORD, 1). It also tracks the count of the number of lines using a Counter. The first reducer then sums up all the values that it received from the first mapper and emits output as (WORD,SUM). The second mapper takes shakespeare data file as input, reads each line and emits a key value pair where key is each word and value is a map with its co-occurence and outputs as (WordX, {WordY1 : 1, WordY2 : 1 ...}). The combiner sums up all the counts of the same key and emits as (WordX, {WordY1:sum, WordY2:sum,...}). The second reducer also sums up the values with respect to the key of the hashmap and fetches the data from the first MapReduce job. And finally it calculates the PMI using the formula and emits output as (WordX, {WordY1 : (PMI, count), WordY2 : (PMI, count)...}).



## Question 2. What is the running time of the complete pairs implementation? What is the running time of the complete stripes implementation?

### Running Time of Pairs Implementation - 4.854 + 21.447 = 26.301 seconds
### Running Time of Stripes Implementaion -  3.855 + 7.505 = 11.36 seconds
#### Ran in linux.student.cs.uwaterloo.ca

## Question 3. Disable all combiners. What is the running time of the complete pairs implementation now? What is the running time of the complete stripes implementation?

### Running Time of Pairs Implementation - 5.655 + 24.463 = 30.118 seconds
### Running Time of Stripes Implementaion -  5.202 + 7.497 = 12.699 seconds
#### Ran in linux.student.cs.uwaterloo.ca

## Question 4. How many distinct PMI pairs did you extract? Use -threshold 10.

### 77198  308792 2327632

## Question 5. What's the pair (x, y) (or pairs if there are ties) with the highest PMI? Use -threshold 10. Similarly, what's the pair with the lowest (negative) PMI? 

### Highest PMI:
#### (maine, anjou)	(3.6331422, 12)
#### (anjou, maine)	(3.6331422, 12)
##### The co-occurence pair (X,Y) and (Y,X) must be in the same line as the code calculates all possible pair of words in a line except the self pair. Hence, their PMI and count is same.
### Lowest PMI:
#### (thy, you)	(-1.5303968, 11)
#### (you, thy)	(-1.5303968, 11)
##### The co-occurence pair (X,Y) and (Y,X) must be in the same line as the code calculates all possible pair of words in a line except the self pair. Hence, their PMI and count is same.

## Question 6. What are the three words that have the highest PMI with "tears" and "death"? -threshold 10. And what are the PMI values?

### Highest PMI with "tears":
#### (tears, shed)	(2.1117902, 15)
#### (tears, salt)	(2.052812, 11)
#### (tears, eyes)	(1.165167, 23)

### Highest PMI with "death":
#### (death, father's)	(1.120252, 21)
#### (death, die)	(0.7541594, 18)
#### (death, life)	(0.73813456, 31)


## Question 7. In the Wikipedia dataset, what are the five words that have the highest PMI with "hockey"? And how many times do these words co-occur? Use -threshold 50

### Highest PMI with "hockey":
#### (hockey, defenceman)	(2.4177904, 153)
#### (hockey, winger)	(2.3697948, 188)
#### (hockey, sledge)	(2.348635, 93)
#### (hockey, goaltender)	(2.2534416, 199)
#### (hockey, ice)	(2.2082422, 2160)

## Question 8. Same as above, but with the word "data".

### Highest PMI with "data":
#### (data, cooling)	(2.0971367, 74)
#### (data, encryption)	(2.0436049, 53)
#### (data, array)	(1.9901568, 50)
#### (data, storage)	(1.9870713, 110)
#### (data, database)	(1.891547, 100)

