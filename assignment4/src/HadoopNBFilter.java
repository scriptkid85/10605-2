import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class HadoopNBFilter extends Configured implements Tool {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();
    
    String labels, content;
    
    String[] labelspace = {"pt","tr","hu","es","ru","pl","ca","nl","sl","fr","ga","de","hr","el"};
    
    String[] labeltokens;

    Vector<String> contenttokens;

    int splitposition;
    

    private static Vector<String> tokenizeDoc(String cur_doc) {
      String[] words = cur_doc.split("\\s+");
      Vector<String> tokens = new Vector<String>();
      for (int i = 0; i < words.length; i++) {
        words[i] = words[i].replaceAll("\\W", "");
        if (words[i].length() > 0) {
          tokens.add(words[i]);
        }
      }
      return tokens;
    }
    
    

    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
      String line = value.toString();

      splitposition = line.indexOf("\t");
      labels = line.substring(0, splitposition);
      content = line.substring(splitposition + 1, line.length());
      if(content.length() <= 5){
        word.set(labels);
        int counter = Integer.parseInt(content);
        context.write(word, new IntWritable(counter));
        return;
      }
      labeltokens = labels.split(",");
      contenttokens = tokenizeDoc(content);

      for (String label : labelspace) {
        for (String token : contenttokens){
        //filter token, denotes the needed events;
        word.set(label + " " + token);
        context.write(word, new IntWritable(-1));
        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
      int sum = 0;
      boolean filtered = true;
      String keypair = key.toString();
      String[] keys = keypair.split(" ");
      if(keys.length == 1){
        filtered = false;
      }
      else{
        if(keys[0].equals("*") || keys[1].equals("*")){
          filtered = false;
        }
      }
      
      if(!filtered){
        for (IntWritable val : values) {
          sum += val.get();
        }
        context.write(key, new IntWritable(sum));
        return;
      }
      
      for (IntWritable val : values) {
        if(val.get() == -1){
          filtered = false;
          continue;
        }
        sum += val.get();
      }
      //filter non-needed events
      if(filtered)return;
      context.write(key, new IntWritable(sum));
    }
  }

  public int run(String[] args) throws Exception {
    Job job = new Job(getConf());
    job.setJarByClass(HadoopNBFilter.class);
    job.setJobName("hadoopnbfilter");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(Integer.parseInt(args[2]));
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    boolean jobCompleted = job.waitForCompletion(true);
    return jobCompleted ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopNBFilter(), args);
    System.exit(result);
  }
}
