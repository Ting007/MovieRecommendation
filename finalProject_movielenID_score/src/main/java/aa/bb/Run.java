package aa.bb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Run {
    private static List<String> target_words = new ArrayList<String>();

    public static class WordMap extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        protected void setup(
                Context context)
                throws IOException, InterruptedException {

            /** 通过conf把传入的值取出来*/
            Configuration conf=context.getConfiguration();
            String keywords=conf.get("keywords");
            String[] key_words=keywords.split(",");
            for ( String word : key_words) {
                target_words.add(word);
                System.out.println(word);
            }
        };

        protected void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            String[] items = value.toString().split(",");

            /** 手动添加list内容 */
            // target_words.add("what");
            // target_words.add("do");
            // target_words.add("to");

            for (String item : items) {

                /** 测试单词统计功能 */
                // word.set(target_words.get(1));//不手动赋值发现get第一个元素
                // 会出现数组越界--说明target_words里面是没有值的
                // context.write(word,one );


                /** 测试contains关键词统计功能 */
                if (target_words.contains(item)) {
                    System.out.println(item);
                    word.set(item);
                    context.write(word, one);
                }

            }
        }

    }

    public static class Myreduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        String job_name = "keywordcount";


        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://bigdata:9000");
        conf.set("mapred.job.tracker", "bigdata:9001");
        if (args.length < 1) {
            System.out
                    .println("Usage: wordcount <input_path> <output_path> <keyword_list>");
            return;
        }
        /** 控制台进行输入 */
        Scanner scanner = new Scanner(System.in);
        String str = scanner.next();
        // String[] target_words = str.split(",");
        // for (String word : target_words) {
        // add(word.toLowerCase());
        // }

        conf.set("keywords", str);//将输入的值存入conf中，以备map取用

        /** 命令行进行输入 */
        // String[] target_words = args[0].split(",");
        // for (String word : target_words) {
        // add(word.toLowerCase());
        // }

        Job job = new Job(conf, job_name);
        job.setJarByClass(Run.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordMap.class);
        job.setReducerClass(Myreduce.class);

         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/user/hadoop/input/"
                + job_name + "/"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/out/"
                + job_name));

        job.waitForCompletion(true);
    }

}