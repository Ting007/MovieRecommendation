package finalProject_java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class run {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		//System.out.println(args[1]);
		if(args[1].equals("1")){//step 1
			String inputName=null;
			File file = new File(args[3]);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				inputName=line.toLowerCase().replaceAll( "\\p{Punct}", "" );
			}
			
			
			
			if(inputName!=null){
				File file2 = new File("middleresult/twoIDs");
				FileReader fileReader2 = new FileReader(file2);
				BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
				String line2;
				boolean matched=false;
				while ((line2 = bufferedReader2.readLine()) != null) {
					if(line2.split("\t")[0].equals(inputName)){
						 File filew = new File(args[4]);
					        FileWriter fw = null;
					        BufferedWriter writer = null;
					            fw = new FileWriter(filew);
					            writer = new BufferedWriter(fw);
					            writer.write(line2.split("\t")[1]);
					            writer.flush();
					            matched=true;
					            break;
					
					}
				}
				
				
				if(matched==false){
					System.out.println("I am sorry, there is no such movie in dataset of wiki");
				}
				
			}else{
				System.out.println("error: no input moive");
			}
		}else if(args[1].equals("2")){//step 2
			File file2 = new File(args[3]);
			FileReader fileReader2 = new FileReader(file2);
			BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
			String line2;
			while ((line2 = bufferedReader2.readLine()) != null) {
				String[] itemArray=line2.split("\t");
				File filew = new File(args[4]);
		        FileWriter fw = null;
		        BufferedWriter writer = null;
		            fw = new FileWriter(filew);
		            writer = new BufferedWriter(fw);
		            
		            String movie="";
		            if(!itemArray[1].isEmpty()){
		            		movie=itemArray[1];
		            }else {
		            		movie="nothing";
		            }
		        
		            
		            String year="";
		            if(!itemArray[2].isEmpty()){
		                year=itemArray[2].substring(0, 4);
		            }else{
		            	year="nothing";
		            }
		            
		            String runtime="";
		            
		            if(!itemArray[3].isEmpty()){
		            
			            if(Float.parseFloat(itemArray[3])>90){
			                runtime="long";
			            }else{
			            	runtime="short";
			            }
		            }else{
		            	runtime="nothing";
		            }
		            
		            
		            String actor="";
		            if(!itemArray[4].isEmpty()){
		                actor=itemArray[4];
		            }else{
		            	actor="nothing";
		            }
		            
		            
		            String language="";
					////this is for language
		            if(!itemArray[5].isEmpty()){
			            String[] langdivided=itemArray[5].split("\"");
						for(int i=0;i<langdivided.length;i++){
							if(!langdivided[i].startsWith("/m")&!langdivided[i].startsWith(",")&!langdivided[i].startsWith(":")){
								language=language+";"+langdivided[i].replace(" language", "").replace("{", "").replace("}", "");
							}
						}
		            }else{
		            	language="nothing";
		            }
		            
					///this is for country
					String country="";		
					if(!itemArray[6].isEmpty()){
			            String[] countrydivided=itemArray[6].split("\"");
						for(int i=0;i<countrydivided.length;i++){
							if(!countrydivided[i].startsWith("/m")&!countrydivided[i].startsWith(",")&!countrydivided[i].startsWith(":")){
								country=country+";"+countrydivided[i].replace("{", "").replace("}", "");
							}
						}
					}else{
						country="nothing";
					}
					
					
					///this is for type
					String type="";
					if(!itemArray[7].isEmpty()){
			            String[] typedivided=itemArray[7].split("\"");
						for(int i=0;i<typedivided.length;i++){
							if(!typedivided[i].startsWith("/m")&!typedivided[i].startsWith(",")&!typedivided[i].startsWith(":")){
								type=type+";"+typedivided[i].replace("{", "").replace("}", "");
							}
						}
					}else{
						type="nothing";
					}
					////this is for content
					String content="";
					if(!itemArray[8].isEmpty()){
					
						content=itemArray[8];
					}else{
						content="nothing";
					}
		            //////write the con
					writer.write("summary:"+content);
					writer.newLine();
					writer.write("actor:"+actor);
					writer.newLine();
					writer.write("year:"+year);
					writer.newLine();
					writer.write("type:"+type);
					writer.newLine();
					writer.write("language:"+language);
					writer.newLine();
					writer.write("country:"+country);
					writer.newLine();
					writer.write("runtime:"+runtime);
					writer.newLine();
					writer.write("movie:"+movie);
		            writer.flush();
			}
			
			
			
		}else if(args[1].equals("3")){//step 3//ugly part
			
			File file1 = new File(args[3]);//we need to know two IDs, step1
			FileReader fileReader1 = new FileReader(file1);
			BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
			String line1;
			HashMap<String,String> wikiIDlenID=new HashMap<String,String>(); 
			while ((line1 = bufferedReader1.readLine()) != null) {
				wikiIDlenID.put((line1.split("\t"))[1], (line1.split("\t"))[2]);
			}
			
			
			File file2 = new File(args[4]);//we need to know wikiscore,step2
			FileReader fileReader2 = new FileReader(file2);
			BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
			String line2;
			int count=0;
			HashMap<String,String> wikiscore=new HashMap<String,String>();
			while ((line2 = bufferedReader2.readLine()) != null) {
				line2.split("\t");
				wikiscore.put((line2.split("\t"))[0], line2.split("\t")[1]);
				//count++;
				//if(count==100){
				//	break;
				//}
			}
			
			File file3 = new File(args[5]);//we keed to know lenscore,step3
			FileReader fileReader3 = new FileReader(file3);
			BufferedReader bufferedReader3 = new BufferedReader(fileReader3);
			String line3;
			HashMap<String,String> lenscore=new HashMap<String,String>();
			while ((line3 = bufferedReader3.readLine()) != null) {
				line3.split("\t");
				lenscore.put((line3.split("\t"))[0], line3.split("\t")[1]);
			}
			
			File filew = new File(args[7]);//this is for the output,step4
	        FileWriter fw = null;
	        BufferedWriter writer = null;
	        fw = new FileWriter(filew);
	            
	        writer = new BufferedWriter(fw);
	            
	        /*compute the average    
	        float aa=0;
	        for (String value: lenscore.values()){
	        	aa+=Float.parseFloat(value);
	        }
	        
	        System.out.println(aa/lenscore.size());    
			*/
	        
	        
			for(String wikiID: wikiscore.keySet()){
				String bb=wikiIDlenID.get(wikiID);//lenID
				if(bb!=null){//may be duplicate name in wiki
				
				
					if(bb.equals("null")){
						Float finalscore=2+Float.parseFloat(wikiscore.get(wikiID));
					    writer.write(wikiID+"	"+String.valueOf(finalscore));
					    writer.newLine();
						
					}else{
						String score=lenscore.get(bb);
						

						if(score==null)//may be duplicate name in moivelen
						{
							Float finalscore=(float)(2.5)+Float.parseFloat(wikiscore.get(wikiID));
						    writer.write(wikiID+"	"+String.valueOf(finalscore));
						    writer.newLine();
						}else{
						
						
							Float finalscore=Float.parseFloat(score)+Float.parseFloat(wikiscore.get(wikiID));
							//System.out.println(score);
							//System.out.println(finalscore);
							
							writer.write(wikiID+"	"+String.valueOf(finalscore));
						    writer.newLine();	
						}
					}
				}
			}
			writer.flush();
			
			
			
			
		}
	}

}
