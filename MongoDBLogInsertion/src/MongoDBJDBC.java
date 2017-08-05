import com.mongodb.*;
import java.lang.Math;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Scanner;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import java.io.File;
public class MongoDBJDBC {
   public static void main( String args[] ) {
      try{
    	  MONGODB_IP = args[0];
    	  MONGODB_PORT = Integer.parseInt(args[1]);
    	  MONGODB_DATABASE = args[2];
    	  MONGODB_COLLECTION = args[3];
    	  FILE_NAME = args[4];
    	  // To connect to mongodb server
    	 System.out.println("Connecting to database...");
         MongoClient mongoClient = new MongoClient( MONGODB_IP , MONGODB_PORT);
         // Now connect to your databases
         DBCollection logCollection = mongoClient.getDB( MONGODB_DATABASE ).getCollection(MONGODB_COLLECTION);
         System.out.println("Connected to database successfully");
         Date date1 = new Date();
         ArrayList<String> lines = new ArrayList(FileUtils.readLines(new File(FILE_NAME)));
         ArrayList<BasicDBObject> documents_batch = new ArrayList();
         int splitBy = 	(int)Math.floor(lines.size()/NO_FILE_SPLITS);
         System.out.println("Transfering file to database...");
         for(int i =0 ;i<NO_FILE_SPLITS-1;i++){
        	 for(int j = i*splitBy; j<(i*splitBy) + (splitBy);j++){
        		 String[] tokens = parseLog(lines.get(j));
        		 BasicDBObject doc = createDocument(tokens);
        		 documents_batch.add(doc);
        	 }
        	 logCollection.insert(documents_batch);
        	 documents_batch.clear();
        		 // System.out.println(i);
        		 //i++;
         };
         for(int i = (NO_FILE_SPLITS-1)*splitBy; i < lines.size(); i++ ){
        	 String[] tokens = parseLog(lines.get(i));
    		 BasicDBObject doc = createDocument(tokens);
    		 documents_batch.add(doc);
         }
    	 logCollection.insert(documents_batch);
    	 documents_batch.clear();
         Date date2 = new Date();
         System.out.println("Transaction finished in " + (date2.getTime() - date1.getTime()) + " milliseconds.");
         mongoClient.close();
         //br.close();
         System.out.println("the log file succensfully transfered to Database!");
      }catch(Exception e){
         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
      }
   }
	
	private static String[] parseLog(String argString) {
		String[] tokens = new String[10];
		boolean escaping = false;
		    char quoteChar = ' ';
		    boolean quoting = false;
		    StringBuilder current = new StringBuilder() ;
		    int tokensCount = 0;
		    for (int i = 0; i<argString.length(); i++) {
		        char c = argString.charAt(i);
		        if (escaping) {
		            current.append(c);
		            escaping = false;
		        } else if (c == '\\' && !(quoting && quoteChar == '\'' )) {
		        	current.append(c);
		        	escaping = true;
		        } else if (quoting && c == quoteChar) {
		            quoting = false;
		        } else if (!quoting && (c == '\'' || c == '"')) {
		            quoting = true;
		            if(argString.charAt(i+1) == '\"'){
		            	quoting = false;
		            	tokens[tokensCount] = "-";
		                tokensCount++;
		            	i++;
		            }
		            quoteChar = c;
		        } else if (!quoting && Character.isWhitespace(c)) {
		            if (current.length() > 0) {
		                tokens[tokensCount] = current.toString();
		                tokensCount++;
		                current = new StringBuilder();
		            }
		        } else {
		            current.append(c);
		        }
		    }
		    if (current.length() > 0) {
		        tokens[tokensCount] = current.toString();
		        tokensCount++;
		    }
		    return tokens;
	}
	private static String[] convertMinusToNULL(String[] tokens){
		for(int i=0; i<tokens.length;i++){
			if(tokens[i].equals("-"))
				tokens[i] = null;
		}
		return tokens;
	}
	private static Date convertTimetoDate(String dateStr) throws ParseException{
		Calendar dayCal = new GregorianCalendar();
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = formatter.parse(dateStr);//Converting Date to timestamp
		return date;
	}
	private static String convertRequestToPath(String reqStr){
		if (reqStr == null) 
				return null;
		String[] splittedReq = reqStr.split(" ");
		String path;
		if(splittedReq.length == 3){
			path = splittedReq[1];
		}else
			path = reqStr;
		return path.toLowerCase();
		}
	private static BasicDBObject createDocument(String[] tokens) throws ParseException{
		tokens = convertMinusToNULL(tokens);
		
		Date date = convertTimetoDate(tokens[TIME].substring(1));
		String path = convertRequestToPath(tokens[REQUEST]);
		BasicDBObject doc = new BasicDBObject("host", (String)tokens[HOST])
				.append("logname", tokens[LOG_NAME])
				.append("user", tokens[USER])
				.append("time", date)
				.append("path", path)
				.append("request", tokens[REQUEST])
				.append("status", (tokens[STATUS] != null)? Integer.parseInt(tokens[STATUS]):tokens[STATUS])
				.append("response_size", (tokens[RESPONSE_SIZE] != null)? Integer.parseInt(tokens[RESPONSE_SIZE]):tokens[RESPONSE_SIZE])
				.append("referrer", tokens[REFERRER])
				.append("user_agent", tokens[USER_AGENT]);		
		return doc;
	}

	private  static String FILE_NAME ;
	private static String MONGODB_IP;
	private  static int MONGODB_PORT;
	private static String MONGODB_DATABASE;
	private static String MONGODB_COLLECTION;
	private static final int HOST = 0;
	private static final int LOG_NAME = 1;
	private static final int USER = 2;
	private static final int TIME = 3;
	private static final int REQUEST = 5;
	private static final int  STATUS = 6;
	private static final int RESPONSE_SIZE = 7;
	private static final int REFERRER = 8;
	private static final int USER_AGENT = 9;
	private static final int NO_FILE_SPLITS = 4;
} 