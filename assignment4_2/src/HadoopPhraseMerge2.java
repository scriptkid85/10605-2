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

public class HadoopPhraseMerge2 extends Configured implements Tool {
  public static class MergeMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, Text> {

    
    //output X \t ^ XY C CXY B BXY and Y \t ^ XY C CXY B BXY
    //    or X \t C CX B BX
    public void map(LongWritable key, Text value, final OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] contents = line.split("\t");
      String[] keys = contents[0].split(" ");
      if(keys.length == 1){
        //identity mapper here for X case
        context.collect(new Text(contents[0]), new Text(contents[1]));
      }
      else if(keys.length == 2){
        //for XY case
        context.collect(new Text(keys[0]), new Text("^ " + contents[0] + " " + contents[1]));
        context.collect(new Text(keys[1]), new Text("^ " + contents[0] + " " + contents[1]));
      }
    }
  }

  public static class MergeReduce extends MapReduceBase implements
          Reducer<Text, Text, Text, Text> {

    Text outputvalue = new Text();
    
    Text outputkey = new Text();

    
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      Vector<String> tempsave = new Vector<String>();
      int BX = 0, CX = 0;
      while (values.hasNext()) {
        String counter = values.next().toString();
        String[] counters = counter.split(" ");
        if(counters.length == 4){
          CX = Integer.parseInt(counters[1]);
          BX = Integer.parseInt(counters[3]);
          break;
        }
        else{
          tempsave.add(counter);
        }
      }
      
      for(String token: tempsave){
        String[] tokens = token.split(" ");
        context.collect(new Text(tokens[1] + " " + tokens[2]), new Text(tokens[4] + " " + tokens[6] + " " + Integer.toString(CX) + " " + Integer.toString(BX)));
      }
      tempsave.clear();
      while (values.hasNext()) {
        String counter = values.next().toString();
        String[] counters = counter.split(" ");
        context.collect(new Text(counters[1] + " " + counters[2]), new Text(counters[4] + " " + counters[6] + " " + Integer.toString(CX) + " " + Integer.toString(BX)));
      }
    }
  }



  protected Path inputfile = null;

  protected Path outputfile = null;

  protected int reducetasks = 2;

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopPhraseMerge2(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

    inputfile = new Path(args[0]);
    outputfile = new Path(args[1]);
    reducetasks = Integer.parseInt(args[2]);

    JobClient.runJob(configHDPMerge2());

    return 0;
  }

  protected JobConf configHDPMerge2() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopPhraseMerge2.class);
    conf.setJobName("HadoopPhraseMerge2");
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
