package aa.bb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
		while ((line = bufferedReader.readLine()) != null) {
			if(line.startsWith("actor")){
				conf.set("actor",line.substring(6));
			}else if(line.startsWith("summary")){
				conf.set("summary",line.substring(8).replaceAll( "\\p{Punct}", "" ));
			}else if(line.startsWith("year")){
				conf.set("year",line.substring(5));
			}else if(line.startsWith("type")){
				conf.set("type",line.substring(5));
			}else if(line.startsWith("language")){
				conf.set("language",line.substring(9));
			}else if(line.startsWith("country")){
				conf.set("country",line.substring(8));
			}else if(line.startsWith("runtime")){
				conf.set("runtime",line.substring(8));
			}else if(line.startsWith("movie")){
				conf.set("movie", line.substring(6));
			}
		}  
		
		
		

		System.out.println("actor:"+conf.get("actor"));
		System.out.println("summary:"+conf.get("summary"));
		System.out.println("year:"+conf.get("year"));
		System.out.println("type:"+conf.get("type"));
		System.out.println("movie:"+conf.get("movie"));
		
		///we need to get the stop list
		File file2 = new File("configuration/english.stop");
		FileReader fileReader2 = new FileReader(file2);
		BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
		String line2;
		String stopstring="";
		
		while ((line2 = bufferedReader2.readLine()) != null) {
			stopstring=stopstring+"divideby"+line2;
		}  
		
        conf.set("stop",stopstring);
		
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
	    job.setOutputValueClass(IntWritable.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static class FinalMap extends Mapper<Object, Text, Text, IntWritable> {
		  private String actor;
		  private ArrayList<String> summaryList=new ArrayList<String>();
		  private String year;
		  private String type;
		  private String stop;
		  private String language;
		  private String country;
		  private String runtime;
		  //private String movieName;
		  private HashSet<String> stopSet=new HashSet<String>();
		  private String firstString;
		  private ArrayList<String> movieList=new ArrayList<String>();
		  private String firstMovie;
		  
		  @Override
		  protected void setup(Context context) throws IOException,InterruptedException {

			  
			  //////get step list and combine a set
			  this.stop=context.getConfiguration().get("stop").toLowerCase();// to lower case
			  
			  String[] stoplist=stop.split("divideby");// this "divideby" is defined by above
              
			  
			  for(int i=0; i<stoplist.length; i++){
				  this.stopSet.add(stoplist[i]);
			  }
			  
			  
			  /////get all others as actor, year, type
			  this.actor=context.getConfiguration().get("actor").toLowerCase();
			  this.year=context.getConfiguration().get("year").toLowerCase();
			  this.type=context.getConfiguration().get("type").toLowerCase();
			  this.language=context.getConfiguration().get("language").toLowerCase();
			  this.country=context.getConfiguration().get("country").toLowerCase();
			  this.runtime=context.getConfiguration().get("runtime").toLowerCase();
			  
			  String movieString=context.getConfiguration().get("movie").toLowerCase();
			  
			  String[] movieWord=movieString.split(" ");
			  for(int i=0; i<movieWord.length; i++) {
				  movieList.add(movieWord[i].toLowerCase());
			  }
			  firstMovie=movieWord[0];
			  
			  String sumaryString=context.getConfiguration().get("summary").toLowerCase();
			  
			  //String[] summary=sumaryString.split(" ");  2017.12.5
			  String[] summary=sumaryString.split("[\\s+\n\t.,;:'\"()?!]"); //2017.12.5
			  
			  for(int i=0; i<summary.length; i++){
				  String summaryItem=summary[i].toLowerCase();
				  if(!stopSet.contains(summaryItem) && summaryItem!=null){//if stopSet has this item from summary, we do continue
					  summaryList.add(summary[i]);
				  }
			  }
			  
			  firstString=summary[0];
			  
          }
  		  
		  @Override
	        public void map(Object offset, Text value, Context context)throws IOException, InterruptedException {

			  
			  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			  
			  
			  
			  /////////////////this is the summary part
			  
			
			  
			  if(fileName.equals("plot_summaries.txt")){//this is for the movie summary file
				  
				  
				  
				  if(!firstString.equals("nothing")){//provided need not be empty
				  
				  
					  String moiveId=(value.toString().split("\t"))[0];
					  String content=(value.toString().split("\t"))[1];
					  
					  String[] wordDataArray=content.split(" ");
					  int score=0;
					  
					  
					  
					  ArrayList<String> wordDataList=new ArrayList<String>();
					  
					  for(int i=0 ; i<wordDataArray.length ; i++){
						  wordDataList.add(wordDataArray[i].toLowerCase());
					  }
					  
					  List<String> ls = new ArrayList<String>(summaryList);
					  ls.retainAll(wordDataList);
					  score = ls.size();
					 if(score>=20 && score < 40) {
						  score = 20;
					  }
					  else if(score>=40 && score<80) {
						  score = 25;
					  }
					  else if(score>=80 ) {
						  score = 30;
					  }
					  
					  float matching=(float)ls.size() /summaryList.size();
					  if(matching>=0.5 && summaryList.size()<40){
						  score+=10;
					  }
					  else if(matching >=0.25 && summaryList.size()<40){
						  score+=8;
					  }
					  else if(matching >=0.1 && summaryList.size()<40){
						  score+=6;
					  }
					  else if(matching >=0.05 && summaryList.size()<40) {
						  score+=4;
					  }
					  
					  IntWritable scoreInt = new IntWritable(score);
					  
					  //System.out.println("moveId : "+moiveId+" , score : "+score);

					  context.write(new Text(moiveId),scoreInt);//send the score to the reduce
				  }
				  
				  
				  
			  ////////////////this part involves the  year, revenue, runtime, language, countries, genres 
			  }else if(fileName.equals("movie.metadata.tsv")){
				  
				  int score=0;
				  
				  String[] itemSplit=(value.toString().split("\t"));
				  
				  
				  String moiveId=itemSplit[0].toLowerCase();
				  String moiveName=itemSplit[2].toLowerCase();
				  String moiveYear=itemSplit[3].toLowerCase();
				  String moiveRevenue=itemSplit[4].toLowerCase();
				  String moiveRuntime=itemSplit[5].toLowerCase();
				  String moiveLanguage=itemSplit[6].toLowerCase();
				  String moiveCountry=itemSplit[7].toLowerCase();
				  String moiveGenres=itemSplit[8].toLowerCase();
				  
				//movie name
				  //System.out.println(firstMovie);
				  if(!firstMovie.equals("nothing")) {
					  if(!moiveName.isEmpty()) {
						  String[] movieNameArray=moiveName.split(" ");
						  ArrayList<String> movieNameList=new ArrayList<String>();
						  
						  for(int i=0 ; i<movieNameArray.length ; i++){
							  movieNameList.add(movieNameArray[i].toLowerCase());
						  }
						  
						  List<String> ls = new ArrayList<String>(movieList);
						  ls.retainAll(movieNameList);
						  //System.out.println(ls.size());
						  //System.out.println(movieList.size());
						  float matching = ((float)ls.size())/movieNameArray.length;
						  if ((matching >= 0.75 | ls.size()==movieList.size()) && movieList.size()>0) {
							  score+=15;
						  }
						  else if (matching > 0.5 && movieList.size()>0) {
							  score+=10;
						  }
						  else if (matching >= 0.25 && movieList.size()>0) {
							  score+=5;
						  }
						  else if(matching >0.1 && movieList.size() > 0) {
							  score+=3;
						  }
						  
					  }
				  }
				  
				  ///year case
				  if(!year.equals("nothing")){//maybe user do not want to fill year
				  
				  
					  if(!moiveYear.isEmpty()){  
						  int yearData=Integer.valueOf(moiveYear.substring(0, 4));// from the data set  
						  String[] yearProvidedArray=year.split(";");//user provide
						  
						  
						  for(int i=0; i<yearProvidedArray.length; i++){
								  int yearProvide=Integer.valueOf(yearProvidedArray[i]);
								  if(yearProvide<=1980){//it is a old movie and matched
									  if(yearData<=1980){
									     score=score+3;
									  }
								  }else{
									  if(Math.abs(yearData-yearProvide)<=2){
										  score=score+5;
									  }else if(Math.abs(yearData-yearProvide)<=5){
										  score=score+3;
									  }else if(Math.abs(yearData-yearProvide)<=8){
										  score=score+1;
									  }
								  }
						  }
						  
					  
					  }
				  }
				  
				  
				  ///runtime case
				  if(!runtime.equals("nothing")){//maybe user do not want to fill year
					  float timeStandrand=90;//90 mintues
					  if(!moiveRuntime.isEmpty()){
						  float runtimeData=Float.parseFloat(moiveRuntime); 
						  if(runtime.equals("long")){
							  if(runtimeData>=timeStandrand){
								  score=score+5;
							  }
						  }else{//short case
							  if(runtimeData<=timeStandrand){
								  score=score+5;
							  }
						  }
					  }
				  }
				  
				  //language case
				  if(!language.equals("nothing")){//like above
					  String[] languageProvidedArray=language.split(";");//user provide
					  for(int i=0;i<languageProvidedArray.length;i++){
						  String langProvided=languageProvidedArray[i];
						  if(moiveLanguage.indexOf(langProvided)!=-1){//moive has this language
							  score=score+8;//language is very important part so 8
						      break;//add only once
						  }
					  }
					  if(moiveId.equals("31186339")){
						  context.write(new Text("language"),new IntWritable(score));//send the score to the reduce
					  }
				  }
				  
				  //country case
				  if(!country.equals("nothing")){//like above
					  String[] countryProvidedArray=country.split(";");//user provide
					  for(int i=0;i<countryProvidedArray.length;i++){
						  String countryProvided=countryProvidedArray[i];
						  if(moiveCountry.indexOf(countryProvided)!=-1){//moive has this country
							  score=score+5;//language is very important part so 8
						      break;//add only once
						  }
					  }
				  }
				  
				  //moiveGenres case// type also means the moive Genres
				  if(!type.equals("nothing")){
					  String[] typeProvidedArray=type.split(";");
					  for(int i=0;i<typeProvidedArray.length;i++){
						  String typeProvided=typeProvidedArray[i];
						  if(moiveGenres.indexOf(typeProvided)!=-1){//moive has this type
							  score=score+5;//language is very important part so 8
						      break;//add only once
						  }
					  }
					  
				  }
				  

				  IntWritable scoreInt = new IntWritable(score);
				  context.write(new Text(moiveId),scoreInt);//send the score to the reduce
				  
			  }else if(fileName.equals("character.metadata.tsv")){//this is for actor match
				  
                  int score=0;
				  
				  String[] itemSplit=(value.toString().split("\t"));
				  
				  String moiveId=itemSplit[0].toLowerCase();
				  String moiveActorName=itemSplit[8].toLowerCase();
				  String[] actorProvidedArray=actor.split(";");
				  for(int i=0;i<actorProvidedArray.length;i++){
					  String actorProvided=actorProvidedArray[i];
					  if(actorProvided.equals(moiveActorName)){
						  if (i<=1) {
							  score=score+8;
						  }
						  else if (i<4) {
							  score=score+5;
						  }
						  else if(i>=4) {
							  score+=3;
						  }
					  }
				  }
				  IntWritable scoreInt = new IntWritable(score);
				  context.write(new Text(moiveId),scoreInt);//send the score to the reduce
			  }
			  
			  
		  }
		  
		  
		  
	  }
	  
	  public static class FinalReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		  @Override
		  public void reduce(Text b, Iterable<IntWritable> values, Context context)//this text is the key of map output, same key will be merged together to values.
			        throws IOException, InterruptedException {
			  
		      int sum = 0;
		      for (IntWritable count : values) {
		        sum += count.get();
		      }
		      context.write(b, new IntWritable(sum));//count the sum
		  }
	  }
	  
}

