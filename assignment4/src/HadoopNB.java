import java.io.IOException;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class HadoopNB extends Configured implements Tool {
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
          Reducer<Text, IntWritable, Text, Text> {

    Text outputvalue = new Text();

    Text outputkey = new Text();

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      String keystring = key.toString();
      String[] keys = keystring.split(" ");
      if (keys.length == 2 && !keys[0].equals("*") && !keys[1].equals("*")) {
        outputkey.set(keys[1]);
        outputvalue.set("* " + keys[0] + " " + Integer.toString(sum));
        context.collect(outputkey, outputvalue);
      } else {
        outputvalue.set("* " + Integer.toString(sum));
        context.collect(key, outputvalue);
      }
    }
  }

  public static class FilterMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, Text> {

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

    public void map(LongWritable key, Text value, final OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      String line = value.toString();

      splitposition = line.indexOf("\t");
      labels = line.substring(0, splitposition);
      content = line.substring(splitposition + 1, line.length());

      // use "*" indicator to classify the test file and counter file
      String[] contents = content.split(" ");
      if (contents.length > 1 && contents[0].equals("*")) {
        word.set(labels);
        String output = contents[1];
        for (int i = 2; i < contents.length; i++) {
          output = output + " " + contents[i];
        }
        context.collect(word, new Text(output));
        return;
      }

      contenttokens = tokenizeDoc(content);

      for (String token : contenttokens) {
        // filter token, denotes the needed events;
        word.set(token);
        context.collect(word, new Text("-1"));
      }
    }
  }

  public static class FilterReduce extends MapReduceBase implements
          Reducer<Text, Text, Text, IntWritable> {

    String tempvalue;

    Text word = new Text();

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> context,
            Reporter reporter) throws IOException {
      HashMap<String, Integer> save = new HashMap<String, Integer>();
      int sum = 0;
      boolean filtered = true;
      String keypair = key.toString();
      String[] keys = keypair.split(" ");
      if (keys.length > 1) {
        filtered = false;
      } else {
        if (keypair.equals("*")) {
          filtered = false;
        }
      }

      if (!filtered) {
        while (values.hasNext()) {
          tempvalue = values.next().toString();
          sum += Integer.parseInt(tempvalue);
        }
        context.collect(key, new IntWritable(sum));
        return;
      }

      while (values.hasNext()) {
        tempvalue = values.next().toString();
        String[] labelandcount = tempvalue.split(" ");
        if (labelandcount.length == 1) {
          if (labelandcount[0].equals("-1")) {
            filtered = false;
          } else
            context.collect(key, new IntWritable(Integer.parseInt(labelandcount[0])));
          continue;
        }
        int tempcount = Integer.parseInt(labelandcount[1]);
        
        //output value is word + label
        String tempvalue = keypair + " " + labelandcount[0]; 
        if (!save.containsKey(tempvalue)) {
          save.put(tempvalue, tempcount);
        } else {
          save.put(tempvalue, save.get(tempvalue) + tempcount);
        }
      }
      // filter non-needed events
      if (filtered)
        return;

      for (String keyinmap : save.keySet()) {
        key.set(keyinmap);
        context.collect(key, new IntWritable(save.get(keyinmap)));
      }
      save.clear();
    }
  }

  protected Path inputtrain = null;

  protected Path inputtest = null;

  protected Path intermediateoutput = null;

  protected Path finaloutput = null;

  protected int reducetasks = 2;

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopNB(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

    inputtrain = new Path(args[0]);
    inputtest = new Path(args[1]);
    intermediateoutput = new Path(args[2]);
    finaloutput = new Path(args[3]);
    reducetasks = Integer.parseInt(args[4]);

    JobClient.runJob(configHDNBCount());
    JobClient.runJob(configHDNBFilter());

    return 0;
  }

  protected JobConf configHDNBCount() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopNB.class);
    conf.setJobName("HadoopNBCount");
    conf.setMapperClass(CountMap.class);
    conf.setReducerClass(CountReduce.class);
    FileInputFormat.setInputPaths(conf, inputtrain);
    FileOutputFormat.setOutputPath(conf, intermediateoutput);

    conf.setNumReduceTasks(reducetasks);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    return conf;
  }

  protected JobConf configHDNBFilter() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopNB.class);
    conf.setJobName("HadoopNBFilter");
    conf.setMapperClass(FilterMap.class);
    conf.setReducerClass(FilterReduce.class);
    Path[] filterfile = { intermediateoutput, inputtest };
    FileInputFormat.setInputPaths(conf, filterfile);
    FileOutputFormat.setOutputPath(conf, finaloutput);

    conf.setNumReduceTasks(reducetasks);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    return conf;
  }

}
