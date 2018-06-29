package aa.bb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FinalProject extends Configured implements Tool {

	  private static final Logger LOG = Logger.getLogger(FinalProject.class);
	  

	  public static void main(String[] args) throws Exception {


		  
	    int res = ToolRunner.run(new FinalProject(), args);
		
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception {

		  
		  
		//// we need to first rad the configuration files
		Configuration conf = new Configuration();  		
		Job job = Job.getInstance(conf, "FinalProject");
	    job.setJarByClass(this.getClass());
	    
    	    String inputDir = args[0];
       	String outputDir = args[1];
    	
	    // Use TextInputFormat, the default unless job.setInputFormatClass is used
	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    job.setMapperClass(FinalMap.class);
	    job.setReducerClass(FinalReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static class FinalMap extends Mapper<Object, Text, Text, Text> {
  		  
		  @Override
	        public void map(Object offset, Text value, Context context)throws IOException, InterruptedException {
			  			  
			  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			  
			  
			  // context.write(new Text("123"), new Text(context.getConfiguration().get("IDs"))); 
			  
			 
			  /////////////////this is the summary part

			  
			  if(fileName.equals("ratings.csv")){//in the moivelen dataset
				  
				  String[] itemArray=value.toString().split(",");
				  String moiveId=itemArray[1];
				  String rating=itemArray[2];
				  
				  context.write(new Text(moiveId), new Text(rating));
				  
			  }
		  }	  
	  }
	  
	  public static class FinalReduce extends Reducer<Text, Text, Text, Text> {
		  @Override
		  public void reduce(Text b, Iterable<Text> values, Context context)//this text is the key of map output, same key will be merged together to values.
			        throws IOException, InterruptedException {
			  		  
			  
			  try{
			  int count=1;
			  float sum=0;
			  for(Text text:values){

				  
				  sum+=Float.parseFloat(text.toString());
				  count++;
				  
			  }
			  
			  float average=sum/count;
			  
			  context.write(b, new Text(String.valueOf(average)));
			  }catch(Exception e){
				  
			  }
	  }
		  }
	  
}

