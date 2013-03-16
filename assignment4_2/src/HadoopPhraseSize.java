import java.io.IOException;
import java.util.*;

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
import org.apache.hadoop.util.*;

public class HadoopPhraseSize extends Configured implements Tool {
  public static class CountMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text value, final OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] contents = line.split("\t");
      if (contents[1].equals("1960")) {
        context.collect(new Text("C* " + contents[0]), new Text(contents[2]));
      } else {
        context.collect(new Text("B* " + contents[0]), new Text(contents[2]));
      }
      context.collect(new Text(contents[0]), new Text("1"));
    }
  }

  public static class CountReduce extends MapReduceBase implements
          Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
      String[] keys = key.toString().split(" ");
      if (!keys[0].equals("B*") && !keys[0].equals("C*")) {
        if (keys.length == 2)
          context.collect(new Text("phrasesize"), new Text("1"));
        else if(keys.length == 1)
          context.collect(new Text("vocabularysize"), new Text("1"));
        return;
      }
      double sum = 0;
      while (values.hasNext()) {
        String counter = values.next().toString();
        double count = Double.parseDouble(counter);
        sum += count;
      }
      if (keys[0].equals("B*")) {
        if (keys.length == 2){
          context.collect(new Text("bvocabularynum"), new Text(Double.toString(sum)));
        }
        else if(keys.length == 3){
          context.collect(new Text("bphrasenum"), new Text(Double.toString(sum)));
        }
      }
      else {
        if (keys.length == 2){
          context.collect(new Text("fvocabularynum"), new Text(Double.toString(sum)));
        }
        else{
          context.collect(new Text("fphrasenum"), new Text(Double.toString(sum)));
        }
      }
    }
  }

  public static class CountMap2 extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, Text> {
    // identity mapper
    public void map(LongWritable key, Text value, final OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] contents = line.split("\t");
      context.collect(new Text(contents[0]), new Text(contents[1]));
    }
  }

  public static class CountReduce2 extends MapReduceBase implements
          Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
      double sum = 0;
      while (values.hasNext()) {
        String counter = values.next().toString();
        double count = Double.parseDouble(counter);
        sum += count;
      }
      context.collect(key, new Text(Double.toString(sum)));
    }
  }
  

  protected Path inputfileu = null;
  
  protected Path inputfileb = null;
  
  protected Path middleoutput = null;

  protected Path outputfile = null;

  protected int reducetasks = 2;

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopPhraseSize(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

    inputfileu = new Path(args[0]);
    inputfileb = new Path(args[1]);
    middleoutput = new Path(args[2]);
    outputfile = new Path(args[3]);
    reducetasks = Integer.parseInt(args[4]);

    JobClient.runJob(configHDPSize1());
    JobClient.runJob(configHDPSize2());
    
    return 0;
  }

  protected JobConf configHDPSize1() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopPhraseSize.class);
    conf.setJobName("HadoopPhraseSize1");
    conf.setMapperClass(CountMap.class);
    conf.setReducerClass(CountReduce.class);
    FileInputFormat.setInputPaths(conf, inputfileu, inputfileb);
    FileOutputFormat.setOutputPath(conf, middleoutput);

    conf.setNumReduceTasks(reducetasks);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }
  
  protected JobConf configHDPSize2() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopPhraseSize.class);
    conf.setJobName("HadoopPhraseSize2");
    conf.setMapperClass(CountMap2.class);
    conf.setReducerClass(CountReduce2.class);
    FileInputFormat.setInputPaths(conf, middleoutput);
    FileOutputFormat.setOutputPath(conf, outputfile);

    conf.setNumReduceTasks(reducetasks);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }
  

}
