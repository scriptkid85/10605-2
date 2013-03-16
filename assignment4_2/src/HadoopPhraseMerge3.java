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

public class HadoopPhraseMerge3 extends Configured implements Tool {
  public static class MergeMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, Text> {

    // identity mapper
    public void map(LongWritable key, Text value, final OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] contents = line.split("\t");
      context.collect(new Text(contents[0]), new Text(contents[1]));

    }
  }

  public static class MergeReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      
      String B = "", C = "";
      if(values.hasNext()){
        String[] counters = values.next().toString().split(" ");
        C = counters[2];
        B = counters[3];
      }
      
      if(values.hasNext()){
        String counter = values.next().toString();
        context.collect(key, new Text(counter + " " + C + " " + B));
      }
     
    }
  }

  protected Path inputfile = null;

  protected Path outputfile = null;

  protected int reducetasks = 2;

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopPhraseMerge3(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

    inputfile = new Path(args[0]);
    outputfile = new Path(args[1]);
    reducetasks = Integer.parseInt(args[2]);

    JobClient.runJob(configHDPMerge3());

    return 0;
  }

  protected JobConf configHDPMerge3() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopPhraseMerge3.class);
    conf.setJobName("HadoopPhraseMerge3");
    conf.setMapperClass(MergeMap.class);
    conf.setReducerClass(MergeReduce.class);
    FileInputFormat.setInputPaths(conf, inputfile);
    FileOutputFormat.setOutputPath(conf, outputfile);

    conf.setNumReduceTasks(reducetasks);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }

}
