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
		  
		  
		File file = new File(args[3]);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line;
		
		int i=0;
		
		String conSend="";
		while ((line = bufferedReader.readLine()) != null) {
            i++;
			if(i>=16){//top 10 movie content is what we want
				break;
			}
				String[] lineItem=line.split("\t");
				
				String conValue=lineItem[0];
				
				System.out.println(conValue);
				conSend=conSend+"divide"+conValue;	
			
				
		}
		
		System.out.println("IDs:"+conSend);
		
		conf.set("IDs",conSend);
		
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

			  String[] IDarray=context.getConfiguration().get("IDs").split("divide");
			  			  
			  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			  
			  HashSet<String> IDset= new HashSet<String>();
			  for(int i=0; i< IDarray.length; i++){
				  IDset.add(IDarray[i]);
			  }
			  
			  
			  // context.write(new Text("123"), new Text(context.getConfiguration().get("IDs"))); 
			  
			 
			  /////////////////this is the summary part

			  
			  if(fileName.equals("plot_summaries.txt")){//this is for the movie summary file
				  String moiveId=(value.toString().split("\t"))[0];
				  String content=(value.toString().split("\t"))[1];
				  
				  if(IDset.contains(moiveId)){
					  context.write(new Text(moiveId),new Text("moiveContent:"+content));//send the score to the reduce
				  }
				  
				  
				  
			  ////////////////this part involves the  year, revenue, runtime, language, countries, genres 
			  }else if(fileName.equals("movie.metadata.tsv")){
				  
				  String[] itemSplit=(value.toString().split("\t"));
				  
				  
				  String moiveId=itemSplit[0].toLowerCase();
				  String moiveName=itemSplit[2].toLowerCase();
				  String moiveYear=itemSplit[3].toLowerCase();
				  String moiveRevenue=itemSplit[4].toLowerCase();
				  String moiveRuntime=itemSplit[5].toLowerCase();
				  String moiveLanguage=itemSplit[6].toLowerCase();
				  String moiveCountry=itemSplit[7].toLowerCase();
				  String moiveGenres=itemSplit[8].toLowerCase();
				  
				  if(IDset.contains(moiveId)){
				  
				  
					  if(IDset.contains(moiveId)){
						  context.write(new Text(moiveId),new Text("moiveId:"+moiveId));//send the score to the reduce
						  context.write(new Text(moiveId),new Text("moiveName:"+moiveName));//send the score to the reduce
						  context.write(new Text(moiveId),new Text("moiveYear:"+moiveYear));//send the score to the reduce
						  context.write(new Text(moiveId),new Text("moiveRuntime:"+moiveRuntime));//send the score to the reduce
						  context.write(new Text(moiveId),new Text("moiveLanguage:"+moiveLanguage));//send the score to the reduce
						  context.write(new Text(moiveId),new Text("moiveCountry:"+moiveCountry));//send the score to the reduce
						  context.write(new Text(moiveId),new Text("moiveGenres:"+moiveGenres));//send the score to the reduce
						  
					  }
				  }
				  
				  
				  
			  }else if(fileName.equals("character.metadata.tsv")){//this is for actor match
				  
                  int score=0;
				  
				  String[] itemSplit=(value.toString().split("\t"));
				  
				  String moiveId=itemSplit[0].toLowerCase();
				  String moiveActorName=itemSplit[8].toLowerCase();
				  
				  if(IDset.contains(moiveId)){
				      context.write(new Text(moiveId),new Text("moiveActor:"+moiveActorName));//send the score to the reduce				  
				  }
			  }

		  }
		  
		  
		  
	  }
	  
	  public static class FinalReduce extends Reducer<Text, Text, Text, Text> {
		  @Override
		  public void reduce(Text b, Iterable<Text> values, Context context)//this text is the key of map output, same key will be merged together to values.
			        throws IOException, InterruptedException {
			  		  
			  String moiveId=null;
			  String moiveName=null;
			  String moiveYear=null;
			  String moiveRuntime=null;
			  String moiveLanguage=null;
			  String moiveCountry=null;
			  String moiveGenres=null;
			  String moiveContent=null;
			  String moiveActor="";
			  
			  
			  
			  for(Text text: values){
		    	  if(text.toString().startsWith("moiveName")){
		    		  moiveName=text.toString().substring(10);
		    	  }else if(text.toString().startsWith("moiveYear")){
		    		  moiveYear=text.toString().substring(10);
		    	  }else if(text.toString().startsWith("moiveRuntime")){
		    		  moiveRuntime=text.toString().substring(13);
		    	  }else if(text.toString().startsWith("moiveActor")){
		    		  moiveActor=moiveActor+";"+text.toString().substring(11);
		    	  }else if(text.toString().startsWith("moiveLanguage")){
		    		  moiveLanguage=text.toString().substring(14);
		    	  }else if(text.toString().startsWith("moiveCountry")){
		    		  moiveCountry=text.toString().substring(13);
		    	  }else if(text.toString().startsWith("moiveGenres")){
		    		  moiveGenres=text.toString().substring(12);
		    	  }else if(text.toString().startsWith("moiveContent")){
		    		  moiveContent=text.toString().substring(13);
		    	  }
		      }
			  
			  context.write(b, new Text(moiveName+"	"+moiveYear+"	"+moiveRuntime+"	"+moiveActor+"	"+moiveLanguage+"	"+moiveCountry+"	"+moiveGenres+"	"+moiveContent));
		  }
	  }
	  
}

