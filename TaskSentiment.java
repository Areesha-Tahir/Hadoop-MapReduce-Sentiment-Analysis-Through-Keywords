import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskSentiment {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{
 private static IntWritable Length = new IntWritable(); //to get length of a comment
 private Text word = new Text(); 	//for tokenisation of read data 
 private Text final_sen = new Text();	//for the final comment that will be passed to the reducer
 int counter = 0;	
 String st = "Text=";	//to check start of comment
 String en = "CreationDate=";	//to check end of the comment
 
 public void map(Object key, Text value, Context context	//mapper function
                 ) throws IOException, InterruptedException {
 	StringTokenizer itr = new StringTokenizer(value.toString());	
 	String new_comment = "";	//for storing the actual comment after cleaning data
     int isFound;	//for finding the start of actual comment
     int endFound;	//for finding the end of actual comment
     boolean found = true;	
     String only_comment = "";
     String word_com = "";
     while (itr.hasMoreTokens()) {	//until there are no more tokens
       word.set(itr.nextToken());	//getting token in word
       isFound = (word.find(st));	//checking if actual comment starts from here
       while (isFound != -1) {	//if start of comment found	
     	  word_com = word.toString();	//convert Text word to string
     	  word_com += " ";	//adding space
     	  only_comment = only_comment + word_com; //appending words in a string
     	  word.set(itr.nextToken());	//getting next token
     	  endFound = (word.find(en)); 	//check if end of comment found
     	  if (endFound != -1) {	//if found
     		  isFound = -1;	//turn loop condition false
     		  found = false;	//and make found as false
     	  }
       }
       if (found == false)	//break the outer loop if end of comment found
     	  break;
     }
     for (int i =6; i < only_comment.length(); i++) {	//constructing the comment after cleaning the data
     	char c = only_comment.charAt(i);
     	new_comment += c;
     }
     counter += 1;	//add 1 to comment number each time
     final_sen = new Text (new_comment);	//put comment in Text form
     Length = new IntWritable(counter);	//put comment number in text form
     context.write(final_sen, Length);	//write in the file
     
 }
}

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  int overall_pos = 0;	//for overall positivity
	  int overall_neg = 0;	//for overall negativity
	  String key_text;		
      String key_word = "overheating";	//key word
      int total_count = 0;	//total count of all the comments
    private IntWritable result = new IntWritable();
    private Text sentiment = new Text();	
    private Text overall_status = new Text();
    private Text token = new Text();
   
    
    //funtion to convert string into string array
    public String[] make_token(String str, int size)
    {

        String[] arrOfStr = str.split("~", size);
        return arrOfStr;
    }
    
    //reducer function 
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    total_count += 1;	//each time new comment is processed add 1
     String positive_arr ="" ;	//to read from positive.txt
     String negative_arr="" ; //to read from negative.txt
     int pos_count = 0;	//to get count of a word in positive.txt
	 int neg_count = 0;  //to get count of a word in negative.txt
     String path1 = "/home/hxn/hadoopMR/positive.txt";
     String path2 = "/home/hxn/hadoopMR/negative.txt";
     //reading positive.txt
     String a; 
	 File kw = new File (path1);		
	 Scanner myReader = new Scanner(kw);
	 while (myReader.hasNextLine()) {
		  a = myReader.nextLine();
		  positive_arr += a;
		  positive_arr += "~";
	  }
	  myReader.close();
	  //reading negative.txt
	  String b;
	  File kw1 = new File (path2);		
	  Scanner myReader1 = new Scanner(kw1);
	  while (myReader1.hasNextLine()) {
		  b = myReader1.nextLine();
		  negative_arr  += b;
		  negative_arr += "~";
	  }
	  myReader1.close();
	//converting the strings from both files into string arrays
  	String[] pos_file = make_token(positive_arr, 2007);
    String[] neg_file = make_token(negative_arr, 4783);
    
    //converting Text comment to string
     key_text = key.toString();
     
     StringTokenizer tokens = new StringTokenizer(key.toString());
     if (key_text.contains(key_word)) { //if key word is found the comment
     while (tokens.hasMoreTokens()) { 	//while there are more words in comment
         token.set(tokens.nextToken());	
         String check = token.toString();	//get word in string
         
         for (int i =0; i < pos_file.length; i++) {	
        	 if (pos_file[i].equals(check)) { //check if word is in positive words
        		 pos_count += 1;	//if found than add 1
        	 }
         }
         for (int i =0; i < neg_file.length; i++) {
        	 if (neg_file[i].equals(check))	//check if word is in negative words
        		 neg_count += 1;	//if found than add 1
         }
     }
     //if there are more positive words 
     if (pos_count > neg_count) {
   	  	overall_pos += 1;		//add1 to overall pos
   	  	sentiment = new Text("Positive Comment");
   	  	overall_status = new Text("Overall the key word is positive");
     }
   //if there are more negative words 	 
     else if (pos_count < neg_count) {
   	  overall_neg += 1; 	//add1 to overall neg
   	  sentiment = new Text("Negative Comment");
   	  overall_status = new Text("Overall the key word is negative");
     }
     
     context.write(key, result);//write original comment in file
     context.write(sentiment, result); // write whether its positive or negative
     }
     if (total_count == 50) {//320371) { //if all comments have been checked
    	 if (overall_neg > overall_pos) {	//if more comments were negative
    		 overall_status = new Text("Overall the key word is negative"); 
    	 }
    	 if (overall_neg < overall_pos) {	//if more comments were positive
    		 overall_status = new Text("Overall the key word is positive"); 
    	 }
    	 context.write(overall_status, result); //write overall sentiment of the key word
     }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task Sentiments");
    job.setJarByClass(TaskSentiment.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
 