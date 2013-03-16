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

public class HadoopPhraseCount extends Configured implements Tool {
  public static class CountMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, final OutputCollector<Text, IntWritable> context,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] contents = line.split("\t");
      
      if(contents[1].equals("1960")){
        context.collect(new Text("C " + contents[0]), new IntWritable(Integer.parseInt(contents[2])));
      }
      else{
        context.collect(new Text("B " + contents[0]), new IntWritable(Integer.parseInt(contents[2])));
      }
    }
  }

  public static class CountReduce extends MapReduceBase implements
          Reducer<Text, IntWritable, Text, IntWritable> {

    Text outputvalue = new Text();

    Text outputkey = new Text();

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> context,
            Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      context.collect(key, new IntWritable(sum));
    }
  }



  protected Path inputfileb = null;

  protected Path inputfileu = null;
  
  protected Path outputfile = null;

  protected int reducetasks = 2;

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopPhraseCount(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

    inputfileb = new Path(args[0]);
    inputfileu = new Path(args[1]);
    outputfile = new Path(args[2]);
    reducetasks = Integer.parseInt(args[3]);

    JobClient.runJob(configHDPCount());

    return 0;
  }

  protected JobConf configHDPCount() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopPhraseCount.class);
    conf.setJobName("HadoopPhraseCount");
    conf.setMapperClass(CountMap.class);
    conf.setReducerClass(CountReduce.class);
    FileInputFormat.setInputPaths(conf, inputfileb, inputfileu);
    FileOutputFormat.setOutputPath(conf, outputfile);

    conf.setNumReduceTasks(reducetasks);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    return conf;
  }


}
