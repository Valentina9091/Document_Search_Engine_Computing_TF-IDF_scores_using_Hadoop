# Document_Search_Engine_Computing_TF-IDF_scores_using_Hadoop

Information retrieval (IR) is concerned with finding material (e.g., documents) of an unstructured nature (usually text) in response to an information need (e.g., a query) from
large collections. One approach to identify relevant documents is to compute scores based on the matches between terms in the query and terms in the documents. For
example, a document with word s such as bal l, team, score, c hampionship is likely to be about sports. It is helpful to define a weight for each term in a document that can be
meaningful for computing such a score. We describe below popular information retrieval metrics such as term frequency, inverse document frequency, and their product, term
frequency-inverse document frequency (TF-IDF), that are used to define weights for terms.

This consists of 5 java files 
-	DocWordCount.java
-	TermFrequency.java
-	TFIDF.java
-	Search.java
-	Rank.java

Note:
Instructions to create input directory:
-	Downloaded and extracted Canterbury.zip
-	Create folder in hdfs
Hadoop fs -mkdir /user/cloudera/input
-	Copy all the files from Canterbury to input directory
Hadoop fs -put <source> <destination>
Hadoop fs -put Canterbury /user/cloudera/input


1.	DocWordCount.java: 
This program is similar to WordCount program with only difference is that we append a delimiter and the filename to each word in a file.
Execution Instructions:
-	Create directory build
Mkdir built
-	Compile the file java file as 
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint
-	Create the jar file as 
jar -cvf docwordcount.jar -C build/ .
-	Delete the output directory if present and Execute the hadoop code jar as
hadoop jar docwordcount.jar org.myorg.DocWordCount /user/cloudera/input /user/cloudera/output 
-	Output file can be found at /user/cloudera/output 
Hadoop fs -ls /user/cloudera/output
-	The output file can be viewed using following command:
Example: hadoop fs -cat /user/cloudera/output/part-r-00000 
-	To get the file on local machine
Hadoop fs -get /user/cloudera/output <current directory>
Hadoop fs -get /user/cloudera/output .


2.	TermFrequency.java: This program counts the words in the each file and returns  logarithmic term Frequency of the word. 

Term frequency is the number of time s a particular word t occurs in a document d.
T F(t, d) = No. of times t appears in document d
Since the importance of a word in a document does not necessarily scale linearly with the frequency of its appearance, a common modification is to instead use the logarithm of the
raw term frequency.
WF(t,d) = 1 + log 10 (TF(t,d)) if TF(t,d) > 0, and 0 otherwise

Execution Instructions:
-	Create empty directory build 
Mkdir built
-	Compile the file java file as 
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrquency.java -d build -Xlint
-	Create the jar file as 
jar -cvf termfrequency.jar -C build/ .
-	Delete the ouput directory if present and Execute the hadoop code jar as
hadoop jar termfrequency.jar org.myorg.TermFrequency /user/cloudera/input /user/cloudera/output 
-	Output file can be found at /user/cloudera/output 
Hadoop fs -ls /user/cloudera/output
-	The output file can be viewed using following command:
Example: hadoop fs -cat /user/cloudera/output/part-r-00000 
-	To get the file on local machine
Hadoop fs -get /user/cloudera/output <current directory>
Hadoop fs -get /user/cloudera/output .

3.	TFIDF.java: This program calculates the TF-IDF score for each word in the file. It consists of two mapreduce jobs, one after another. The first mapreduce job computes the Term Frequency and second job takes the output files of the first job as input and computes TF-IDF values

Inverse Document Frequency:
The inverse document frequency (IDF) is a measure of how common or rare a term is across all documents in the collection. It is the logarithmically scaled fraction of the
documents that contain the word, and is obtained by taking the logarithm of the ratio of the total number of documents to the number of documents containing the term.
IDF(t) = log 10 (Total # of documents / # of documents containing term t) 

Under this IDF formula, terms appearing in all documents are assumed to be stopwords and subsequently assigned IDF=0. We will use the smoothed version of this formula as follows:
IDF(t) = log 10 (1 + Total # of documents / # of documents containing term t) 

TF-IDF:
Term frequency–inverse document frequency (TF-IDF) is a numerical statistic that is intended to reflect how important a word is to a document in a collection or corpus of
documents. It is often used as a weighting factor in information retrieval and text mining.
TF-IDF(t, d) = WF(t,d) * IDF(t)

Execution Instructions:
-	Create empty directory build 
Mkdir built
-	Compile the file java file as 
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrquency.java TFIDF.java -d build -Xlint
-	Create the jar file as 
jar -cvf tfidf.jar -C build/ .
-	Delete the ouput directory if present and Execute the hadoop code jar as
hadoop jar tfidf.jar org.myorg.TFIDF /user/cloudera/input /user/cloudera/output_1 /user/cloudera/output_2
-	Output file can be found at /user/cloudera/output_2 
Hadoop fs -ls /user/cloudera/output_2
-	The output file can be viewed using following command:
Example: hadoop fs -cat /user/cloudera/output_2/part-r-00000 
-	To get the file on local machine
Hadoop fs -get /user/cloudera/output_2 <current directory>
Hadoop fs -get /user/cloudera/output_2 .

4.	Search.java: This program implements a simple batch mode search engine. The job (Search.java) accepts as input a user query and outputs a list of documents with scores that best matches the query
Execution Instructions:
-	Create empty directory build 
Mkdir built
-	Compile the file java file as 
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint
-	Create the jar file as 
jar -cvf search.jar -C build/ .
-	Delete the ouput directory if present and Execute the hadoop code jar as
hadoop jar search.jar org.myorg.Search /user/cloudera/<output of TFIDF> /user/cloudera/output <user query>
example: hadoop jar search.jar org.myorg.Search /user/cloudera/<output of TFIDF> /user/cloudera/output Computer Science

Note here input is the output of Class TFIDF
-	Output file can be found at /user/cloudera/output
Hadoop fs -ls /user/cloudera/output
-	The output file can be viewed using following command:
Example: hadoop fs -cat /user/cloudera/output/part-r-00000 
-	To get the file on local machine
Hadoop fs -get /user/cloudera/output <current directory>
Hadoop fs -get /user/cloudera/output .

5.	Rank.java: This program ranks the search hits in descending order using their accumulated score
Execution Instructions:
-	Create empty directory build 
Mkdir built
-	Compile the file java file as 
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Rank.java -d build -Xlint
-	Create the jar file as 
jar -cvf rank.jar -C build/ .
-	Delete the ouput directory if present and Execute the hadoop code jar as
hadoop jar rank.jar org.myorg.Rank <path of output of Search class> /user/cloudera/output 
Note here input is the output of Class Search
-	Output file can be found at /user/cloudera/output
Hadoop fs -ls /user/cloudera/output
-	The output file can be viewed using following command:
Example: hadoop fs -cat /user/cloudera/output/part-r-00000 
-	To get the file on local machine
Hadoop fs -get /user/cloudera/output <current directory>
Hadoop fs -get /user/cloudera/output .
