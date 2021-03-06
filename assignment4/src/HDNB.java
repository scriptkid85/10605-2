import java.io.IOException;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class HDNB extends Configured implements Tool {

  public static class CountMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, IntWritable> {

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

    public void map(LongWritable key, Text value, final OutputCollector<Text, IntWritable> context,
            Reporter reporter) throws IOException {
      String line = value.toString();

      splitposition = line.indexOf("\t");
      labels = line.substring(0, splitposition);
      content = line.substring(splitposition + 1, line.length());
      labeltokens = labels.split(",");
      contenttokens = tokenizeDoc(content);

      for (String label : labeltokens) {

        // Y = ANY
        word.set("*");
        context.collect(word, one);

        // Y = label
        word.set(label);
        context.collect(word, one);

        for (String token : contenttokens) {

          // Y= label, X = token
          word.set(label + " " + token);
          context.collect(word, one);

          // Y= label, X = any
          word.set(label + " *");
          context.collect(word, one);

          // Y= any, X = any
          word.set("* *");
          context.collect(word, one);

        }
      }
    }
  }

  public static class CountReduce extends MapReduceBase implements
          Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> context, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      context.collect(key, new IntWritable(sum));
    }
  }

  public static class FilterMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();

    String labels, content;

    String[] labelspace = { "pt", "tr", "hu", "es", "ru", "pl", "ca", "nl", "sl", "fr", "ga", "de",
        "hr", "el" };

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

    public void map(LongWritable key, Text value, final OutputCollector<Text, IntWritable> context,
            Reporter reporter) throws IOException {
      String line = value.toString();

      splitposition = line.indexOf("\t");
      labels = line.substring(0, splitposition);
      content = line.substring(splitposition + 1, line.length());
      if (content.length() <= 5) {
        word.set(labels);
        int counter = Integer.parseInt(content);
        context.collect(word, new IntWritable(counter));
        return;
      }
      labeltokens = labels.split(",");
      contenttokens = tokenizeDoc(content);

      for (String label : labelspace) {
        for (String token : contenttokens) {
          // filter token, denotes the needed events;
          word.set(label + " " + token);
          context.collect(word, new IntWritable(-1));
        }
      }
    }
  }

  public static class FilterReduce extends MapReduceBase implements
          Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> context, Reporter reporter) throws IOException {
      int sum = 0;
      boolean filtered = true;
      String keypair = key.toString();
      String[] keys = keypair.split(" ");
      if (keys.length == 1) {
        filtered = false;
      } else {
        if (keys[0].equals("*") || keys[1].equals("*")) {
          filtered = false;
        }
      }

      if (!filtered) {
        while (values.hasNext()) {
          sum += values.next().get();
        }
        context.collect(key, new IntWritable(sum));
        return;
      }

      while (values.hasNext()) {
        int tempcount = values.next().get();
        if (tempcount == -1) {
          filtered = false;
          continue;
        }
        sum += tempcount;
      }
      // filter non-needed events
      if (filtered || sum == 0)
        return;
      context.collect(key, new IntWritable(sum));
    }
  }

//  protected String bucket = null;
  
  protected Path inputtrain = null;
  
  protected Path inputtest = null;

  protected Path intermediateoutput = null;

  protected Path finaloutput = null;

  protected int reducetasks = 2;

//  FileSystem fs;

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HDNB(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

//    bucket = args[0];
    inputtrain = new Path(args[0]);
    inputtest = new Path(args[1]);
    intermediateoutput = new Path(args[2]);
    finaloutput = new Path(args[3]);
    reducetasks = Integer.parseInt(args[4]);
    
    

//    fs = FileSystem.get(URI.create(bucket), getConf());
    JobClient.runJob(configHDNBCount());
    JobClient.runJob(configHDNBFilter());

    return 0;
  }

  protected JobConf configHDNBCount() throws Exception {
    final JobConf conf = new JobConf(getConf(), HDNB.class);
    conf.setJobName("HadoopNBCount");
    conf.setMapperClass(CountMap.class);
    conf.setReducerClass(CountReduce.class);

//    fs.delete(intermediateoutput, true);

    FileInputFormat.setInputPaths(conf, inputtrain);
    FileOutputFormat.setOutputPath(conf, intermediateoutput);

    conf.setNumReduceTasks(reducetasks);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    return conf;
  }

  protected JobConf configHDNBFilter() throws Exception {
    final JobConf conf = new JobConf(getConf(), HDNB.class);
    conf.setJobName("HadoopNBFilter");
    conf.setMapperClass(FilterMap.class);
    conf.setReducerClass(FilterReduce.class);

 //   fs.delete(finaloutput, true);

    Path[] filterfile = {intermediateoutput, inputtest};
    FileInputFormat.setInputPaths(conf, filterfile);
    FileOutputFormat.setOutputPath(conf, finaloutput);

    conf.setNumReduceTasks(reducetasks);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    return conf;
  }

}
