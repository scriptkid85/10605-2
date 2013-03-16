import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class HadoopNBCounter extends Configured implements Tool {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();

    String labels, content;

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
      labeltokens = labels.split(",");
      contenttokens = tokenizeDoc(content);

      for (String label : labeltokens) {

        // Y = ANY
        word.set("*");
        context.write(word, one);

        // Y = label
        word.set(label);
        context.write(word, one);

        for (String token : contenttokens) {

        //Y= label, X = token
          word.set(label + " " + token);
          context.write(word, one);

          //Y= label, X = any
          word.set(label + " *");
          context.write(word, one);
          
          //Y= any, X = any
          word.set("* *");
          context.write(word, one);

        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public int run(String[] args) throws Exception {
    Job job = new Job(getConf());
    job.setJarByClass(HadoopNBCounter.class);
    job.setJobName("hadoopnbcounter");
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
    int result = ToolRunner.run(new HadoopNBCounter(), args);
    System.exit(result);
  }
}
