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

public class HadoopPhraseRank extends Configured implements Tool {
  public static class MergeMap extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, Text> {

    double  fphrasenum = 0, bphrasenum = 0,  fvocabularynum = 0;
    double vocabularysize = 0, phrasesize = 0;
    
    public void configure(JobConf job) {
      fphrasenum = Double.parseDouble(job.get("fphrasenum")); 
      bphrasenum = Double.parseDouble(job.get("bphrasenum")); 
      fvocabularynum = Double.parseDouble(job.get("fvocabularynum")); 
      phrasesize = Double.parseDouble(job.get("phrasesize")); 
      vocabularysize = Double.parseDouble(job.get("vocabularysize")); 
    }
        
    private static double computeKL(double p, double q){
      return p * Math.log(p / q);
    }
    
    private double[] computeScore(String s){
      double[] PI = new double[2];
      String[] values = s.split(" ");
      long CXYcount = Long.parseLong(values[0]);
      long BXYcount = Long.parseLong(values[1]);
      

      long CXcount = Long.parseLong(values[2]);

      long CYcount = Long.parseLong(values[4]);
      
      double PfgXY = (CXYcount + 1) / (double)(fphrasenum + phrasesize);
      double PbgXY = (BXYcount + 1) / (double)(bphrasenum + phrasesize);
      double PfgX = (CXcount + 1) / (double)(fvocabularynum + vocabularysize);
      double PfgY = (CYcount + 1) / (double)(fvocabularynum + vocabularysize);
      
      double Phraseness = computeKL(PfgXY, PfgX * PfgY);
      double Informativeness = computeKL(PfgXY, PbgXY);
      PI[0] = Phraseness;
      PI[1] = Informativeness;
      return PI;
   }
    

    public void map(LongWritable key, Text value, final OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] contents = line.split("\t");
      
      double[] score = computeScore(contents[1]);
      double totalscore = score[0] + score[1];
      
      context.collect(new Text(String.format("%.8f", (totalscore))), new Text(contents[0] + "\t" + String.format("%.3f", score[0]) + "\t" + String.format("%.3f", score[1])));
    }
  }

  public static class MergeReduce extends MapReduceBase implements
          Reducer<Text, Text, Text, Text> {

    //identity reducer
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context,
            Reporter reporter) throws IOException {
      while (values.hasNext()) {
        context.collect(key, values.next());
      }
    }
  }



  protected Path inputfile = null;

  protected Path outputfile = null;

  protected int reducetasks = 1;
  
  String  fphrasenum = "", bphrasenum = "",  fvocabularynum = "";
  String vocabularysize = "", phrasesize = "";

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new HadoopPhraseRank(), args);
    System.exit(result);
  }

  public int run(String[] args) throws Exception {

    inputfile = new Path(args[0]);
    outputfile = new Path(args[1]);
    fphrasenum = args[2];
    bphrasenum = args[3];
    fvocabularynum = args[4];
    vocabularysize = args[5];
    phrasesize = args[6];
    
    JobClient.runJob(configHDPRank());

    return 0;
  }

  protected JobConf configHDPRank() throws Exception {
    final JobConf conf = new JobConf(getConf(), HadoopPhraseRank.class);
    
    conf.set("fphrasenum", "" + fphrasenum);
    conf.set("bphrasenum", "" + bphrasenum);
    conf.set("fvocabularynum", "" + fvocabularynum);
    conf.set("vocabularysize", "" + vocabularysize);
    conf.set("phrasesize", "" + phrasesize);
    
    conf.setJobName("HadoopPhraseRank");
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
