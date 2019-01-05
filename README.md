# MongoDBExamples  
Our goal is inserting a large log file into the Mongo database and perform some queries.  
This is an example program of interfacing with mongoDB using java  
Here you can find the problem description  
https://docs.mongodb.com/ecosystem/use-cases/storing-log-data  

## First program (MongoDBLogInsertion)  
is used to insert the large log file into the mongo database. (the file named "access_log")  

Here's an example of a log record  
```
189.123.184.39 - - [01/Jan/2017:14:23:59 -0500] "POST /command.php HTTP/1.0" 404 10076 "-" "Wget(linux)"
```

and that is how it's stored in the database  
```
{
"_id" : ObjectId("59852c11c8ceb436300ba790"),
"host" : "189.123.184.39",
"logname" : null,
"user" : null,
"time" : ISODate("2017-01-01T14:23:59Z"),
"path" : "/command.php",
"request" : "POST /command.php HTTP/1.0",
"status" : 404,
"response_size" : 10076,
"referrer" : null,
"user_agent" : "Wget(linux)"
}
```  

### How to Use:  
1. first make sure you installed mongoDB on your machine. https://docs.mongodb.com/manual/installation/  
2. Create a database.  
3. clone the repo https://github.com/AbdelrahmanAbdelrazek/MongoDBExamples  
4. Execute the MongoDBLogInsertion.jar file 
```
java -jar MongoDBLogInsertion.jar 'IP Address' 'MongoDB Port' 'Database name' 'Collection name' 'log file directory'
```
Example  
```
java -jar MongoDBLogInsertion.jar 172.0.0.1 27017 testlocaldb logs ./access_log  
```


## Second program (ExampleQueries)  
executes some basic querries on that database like:  
1. Finding All Events for a Particular Page.  
2. Finding All the Events for a Particular Date.  
3. Finding All Events for a Particular Host/Date.  
4. Counting number of requests for each page in a particular day.  
The output will be written on a file of your choice.  

### How to Use:  
1. make sure the file is transfered into your database using the first program.  
2. Execute the ExampleQueries.jar file  
```
java -jar ExampleQueries.jar 'IP Address' 'MongoDB Port' 'Database name' 'Collection name' 'output file directory'  
```
Example
```
java -jar ExampleQueries.jar 172.0.0.1 27017 testlocaldb logs ./out.txt
```
3. choose which querry you want to execute and enter the required fields.  
4. The results will be written in the file you specified. 
