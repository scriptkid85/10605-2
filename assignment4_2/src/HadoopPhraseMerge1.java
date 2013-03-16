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

public class HadoopPhraseMerge1 extends Configured implements Tool {
  public static class MergeMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, final OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] contents = line.split("\t");
      String[] keys = contents[0].split(" ");
      if(keys[0].equals("C*")){
        if(keys.length == 2)
          context.collect(new Text(keys[1]), new Text("C* " + contents[1]));
        else if(keys.length == 3)
          context.collect(new Text(keys[1] + " " + keys[2]), new Text("C* " + contents[1]));
      }
      else{
        if(keys.length == 2)
          context.collect(new Text(keys[1]), new Text("B* " + contents[1]));
        else if(keys.length == 3)
          context.collect(new Text(keys[1] + " " + keys[2]), new Text("B* " + contents[1]));
      }
    }
  }

  public static class MergeReduce extends MapReduceBase implements
          Reducer<Text, Text, Text, Text> {
    
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      
      String B = "", C = "";
      while (values.hasNext()) {
        String counter = values.next().toString();
        String[] counters = counter.split(" ");
        if(counters[0].equals("C*")){
          C = counters[1];
        }
        else if(counters[0].equals("B*")){
          B = counters[1];
        }
      }
      context.collect(key, new Text("C* " + C + " B* " + B));
    }
  }



  protected Path inputfile = null;

  protected Path outputfile = null;

  protected int reducetasks = 2;

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopPhraseMerge1(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

    inputfile = new Path(args[0]);
    outputfile = new Path(args[1]);
    reducetasks = Integer.parseInt(args[2]);

    JobClient.runJob(configHDPMerge1());

    return 0;
  }

  protected JobConf configHDPMerge1() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopPhraseMerge1.class);
    conf.setJobName("HadoopPhraseMerge1");
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
