import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Date;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;

import org.bson.Document;
public class Query {
	public static void main(String args[]) {
		try{
			MONGODB_IP = args[0];
			MONGODB_PORT = Integer.parseInt(args[1]);		
			MONGODB_DATABASE = args[2];
			MONGODB_COLLECTION = args[3];
			FILE_NAME = args[4];
			int choiceInput = promptForQuery();
			if(choiceInput == 1){
				String pageInput = promptForPage();
				findEventsForAPath(pageInput);
			}
			else{
				Date dateInput =promptForDate();
				Calendar cal = Calendar.getInstance();
			    cal.setTime(dateInput);
			    int year = cal.get(Calendar.YEAR);
			    int month = cal.get(Calendar.MONTH);
			    int day = cal.get(Calendar.DAY_OF_MONTH);
			    if(choiceInput == 2)
			    	findEventsForDate(year, month, day);
			    else if( choiceInput == 3){
			    	String ipInput = promptForIP();
			    	findEventsForHostDate(ipInput, year, month, day);
			    }
			    else if (choiceInput == 4){
			    	countingHitsPerPageInDay(year, month, day);
			    }
			} 
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
	}
	
	private static void PromptForMongoServerDetaills() {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter output file name: ");
		
		System.out.print("Enter the Mongo database name: ");
		MONGODB_DATABASE = scanner.nextLine();
		System.out.print("Enter the Mongo database collection name: ");
		
		System.out.print("Enter the Mongo database server IP: ");
		
		System.out.print("Enter the Mongo database server Port (Defauly 27017): ");
		while(!scanner.hasNextInt()) {
		    scanner.next();
		}
	}

	private static int promptForQuery() {
		Scanner scanner = new Scanner(System.in);
		int choice = 0;
		System.out.println("Choose which query to perfom:");
		System.out.println("1.Find all events for a particular Page");
		System.out.println("2.Find all the events for a particular Date: ");
		System.out.println("3.Find all events for a particular Host/Date.");
		System.out.println("4.Counting the number of Requests for each page in a particular day.");
		while(choice<1 || choice >4){
			while(!scanner.hasNextInt()) {
			    scanner.next();
			}
			choice = scanner.nextInt();
		}
		return choice;
	}

	private static String promptForPage() {
		System.out.println("Please, enter the desired page: (Example: /favicon.ico )");
		Scanner scanner = new Scanner(System.in);
		String pageInput = scanner.nextLine();
		return pageInput;
	}

	private static String promptForIP() {
		System.out.println("Please, enter the desired host IP: (Example: 37.157.246.146 )");
		Scanner scanner = new Scanner(System.in);
		String ipInput = scanner.nextLine();
		return ipInput;
	}
	public static Date promptForDate(){
		Scanner scanner = new Scanner(System.in);
		int year=0,month=0,day=0;
		HashMap<String,Integer> out;
		System.out.println("Please, enter the desired date:");
		System.out.print("Enter the year: ");
		while(!scanner.hasNextInt()) {
		    scanner.next();
		}
		year = scanner.nextInt();
		System.out.print("Enter the month: ");
		while(!scanner.hasNextInt()) {
		    scanner.next();
		}
		month = scanner.nextInt();
		System.out.print("Enter the day: ");
		while(!scanner.hasNextInt()) {
		    scanner.next();
		}
		day = scanner.nextInt();
		return new GregorianCalendar(year,month,day).getTime();
	}
	public static void findEventsForAPath(String path) throws IOException{
		File fout = new File(FILE_NAME);
		FileOutputStream fos = new FileOutputStream(fout, false);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		MongoClient mongoClient = new MongoClient( MONGODB_IP, MONGODB_PORT );
        DBCollection logCollection = mongoClient.getDB( MONGODB_DATABASE).getCollection(MONGODB_COLLECTION);
        DBCursor cursor = logCollection.find(new BasicDBObject("path",path));
		int i=1;
        while (cursor.hasNext()) {
        	DBObject c = cursor.next();
        	System.out.println(i + "|" + c);
        	bw.write(i + "|" + c);
        	bw.newLine();
        	i++;
            }
        bw.close();
        mongoClient.close();
        System.out.println("Query Successfully Executed!, results are written in " + FILE_NAME);
	}
	public static void findEventsForDate(int year, int month, int day) throws ParseException, IOException{
		File fout = new File(FILE_NAME);
		FileOutputStream fos = new FileOutputStream(fout, false);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		Calendar dayCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		dayCal.set(year, month-1, day,0,0,0);
		Calendar nextDayCal = (Calendar) dayCal.clone();
		nextDayCal.add(Calendar.DAY_OF_WEEK, 1);	
		MongoClient mongoClient = new MongoClient( MONGODB_IP, MONGODB_PORT );
        DBCollection logCollection = mongoClient.getDB( MONGODB_DATABASE ).getCollection(MONGODB_COLLECTION);
		DBObject query = new QueryBuilder().start().put("time")
				.greaterThanEquals(dayCal.getTime())
				.lessThan(nextDayCal.getTime()).get();
		DBCursor cursor = logCollection.find(query);
		int i=1;
        while (cursor.hasNext()) {
        	DBObject c = cursor.next();
        	System.out.println(i + "|" + c);
        	bw.write(i + "|" + c);
        	bw.newLine();
        	i++;
            }
        bw.close();
        mongoClient.close();
        System.out.println("Query Successfully Executed!, results are written in " + FILE_NAME);
	}
	public static void findEventsForHostDate(String host, int year, int month, int day) throws ParseException, IOException{
        File fout = new File(FILE_NAME);
		FileOutputStream fos = new FileOutputStream(fout,false);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		Calendar dayCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		dayCal.set(year, month-1, day,0,0,0);
		Calendar nextDayCal = (Calendar) dayCal.clone();
		nextDayCal.add(Calendar.DAY_OF_WEEK, 1);
		MongoClient mongoClient = new MongoClient( MONGODB_IP, MONGODB_PORT );
        DBCollection logCollection = mongoClient.getDB( MONGODB_DATABASE).getCollection(MONGODB_COLLECTION);
		DBObject query = new QueryBuilder().start().put("host").is(host).put("time")
				.greaterThanEquals(dayCal.getTime())
				.lessThan(nextDayCal.getTime()).get();
		DBCursor cursor = logCollection.find(query);
		int i=1;
        while (cursor.hasNext()) {  
        	DBObject c = cursor.next();
        	System.out.println(i + "|" + c);
        	bw.write(i + "|" + c);
        	bw.newLine();
        		i++;
            }
        bw.close();
        mongoClient.close();
        System.out.println("Query Successfully Executed!, results are written in " + FILE_NAME);
	}
	public static void countingHitsPerPageInDay(int year,  int month, int day) throws IOException{
		Calendar dayCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		dayCal.set(year, month-1, day,0,0,0);
		Calendar nextDayCal = (Calendar) dayCal.clone();
		nextDayCal.add(Calendar.DAY_OF_WEEK, 1);	
		MongoClient mongoClient = new MongoClient( MONGODB_IP , MONGODB_PORT);
		MongoDatabase database = mongoClient.getDatabase(MONGODB_DATABASE);
		MongoCollection<Document> logCollection = database.getCollection(MONGODB_COLLECTION);
		Document timeDocument = new Document("$gte", dayCal.getTime())
				.append("$lt", nextDayCal.getTime());
		Document matchDocument = new Document("time",timeDocument);
		Document projectDocument = new Document("path",1);
		Document groupDocument = new Document("_id",new Document("page","$path"))
				.append("hits", new Document("$sum",1));
		List<Document> pipeline = Arrays.asList(
				new Document("$match", matchDocument),
				new Document("$project", projectDocument),
				new Document("$group", groupDocument)
				);
        AggregateIterable<Document> output = logCollection.aggregate(pipeline);
        File fout = new File(FILE_NAME);
		FileOutputStream fos = new FileOutputStream(fout,false);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        int i = 1;
        for(Document doc: output){
        	Document _idDoc = (Document) doc.get("_id");
        	int hits = (int)doc.get("hits");
        	String pageStr = _idDoc.getString("page");
        	System.out.println(i+"|"+"Page:"+ pageStr + '\t' + "Hits: "+hits);
        	bw.write(i+"|"+"Page:"+ pageStr + '\t' + "Hits: "+hits);
    		bw.newLine();
        	i++;
        	}
        bw.close();
        mongoClient.close();
        System.out.println("Query Successfully Executed!, results are written in " + FILE_NAME);
        }

	private  static String FILE_NAME;
	private static String MONGODB_IP;
	private  static int MONGODB_PORT;
	private static String MONGODB_DATABASE;
	private static String MONGODB_COLLECTION;
}