import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.util.Vector;

import com.sun.org.apache.xpath.internal.operations.Equals;

public class MemEffiSGDwoBias {

  static long N = 100000;

  static long k = 0;

  static double eta = 0.5;

  static double mu = 0.1;

  static double overflow = 20;

  static int labelsize = 14;

  static long trainsize = 0;
  
  static String inputtrainfile = null;
  
  static String inputtestfile = null;
  
  static boolean verbose = false;
  
  static String[] wholelabels = {"nl","el","ru","sl","pl","ca","fr","tr","hu","de","hr","es","ga","pt"};

  @SuppressWarnings("unchecked")
  static Hashtable<Long, Long>[] A = (Hashtable<Long, Long>[]) new Hashtable<?, ?>[labelsize];

  @SuppressWarnings("unchecked")
  static Hashtable<Long, Double>[] B = (Hashtable<Long, Double>[]) new Hashtable<?, ?>[labelsize];

  static Hashtable<String, Integer> labelspace = new Hashtable<String, Integer>();

  private static void Init() {
    k = 0;

    for (int i = 0; i < labelsize; i++) {
      A[i] = new Hashtable<Long, Long>();
      for (long j = 0; j < N; j++) {
        A[i].put(j, (long) 0);
      }
      B[i] = new Hashtable<Long, Double>();
    }
    labelspace.put("nl", 0);
    labelspace.put("el", 1);
    labelspace.put("ru", 2);
    labelspace.put("sl", 3);
    labelspace.put("pl", 4);
    labelspace.put("ca", 5);
    labelspace.put("fr", 6);
    labelspace.put("tr", 7);
    labelspace.put("hu", 8);
    labelspace.put("de", 9);
    labelspace.put("hr", 10);
    labelspace.put("es", 11);
    labelspace.put("ga", 12);
    labelspace.put("pt", 13);
  }

  private static void outputWeight(){
    for(int i = 0; i < labelsize; ++i){
      for(long code: B[i].keySet()){
        System.out.println("[" + wholelabels[i] + "] " + code + ": " + B[i].get(code));
      }
    }
  }
  
  
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

  protected static double sigmoid(double score) {
    if (score > overflow)
      score = overflow;
    else if (score < -overflow)
      score = -overflow;
    double exp = Math.exp(score);
    return exp / (1 + exp);
  }

  protected static long hashcode(String word) {
    long id = word.hashCode() % N;
    if (id < 0)
      id += N;
    return id;
  }
  
  
  
  private static void penalWeight(long k){
    int iteration = (int) (Math.floor( k / trainsize) + 1);
    for(int labelcode = 0; labelcode < labelsize; ++labelcode){
      for(long code: B[labelcode].keySet()){
        double lambda = eta/ (iteration * iteration);
        double newvalue = B[labelcode].get(code)
                * Math.pow((1 - 2 * lambda * mu), k - A[labelcode].get(code));
        B[labelcode].put(code, newvalue);
        A[labelcode].put(code, k);
      }
    }
  }
  
  
  
  //outputoverallLCL() is used in Q1 for outputing loglikelihood
  protected static void outputoverallLCL(long k){
    penalWeight(k);
    double ret = 0;
    try {
      // Open the file that is the first
      // command line parameter
      FileInputStream fstream = new FileInputStream(inputtrainfile);
      // Get the object of DataInputStream
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));

      String s, labels, content;
      String[] labeltokens;
      Vector<String> contenttokens;
      Hashtable<Long, Integer> V = new Hashtable<Long, Integer>();
      int splitposition;

      while ((s = br.readLine()) != null && s.length() != 0) {
        
        splitposition = s.indexOf("\t");
        labels = s.substring(0, splitposition);
        content = s.substring(splitposition + 1, s.length());
        labeltokens = labels.split(",");
        contenttokens = tokenizeDoc(content);
        
        V.clear();
        for (String token : contenttokens) {
          long Code = hashcode(token);
          if (V.containsKey(Code)) {
            V.put(Code, V.get(Code) + 1);
          } else {
            V.put(Code, 1);
          }
        }
        
        
        int positive[] = new int[labelsize];
        // deal with positive labels first
        for (String label : labeltokens) {
          if (!labelspace.containsKey(label)) {
            System.out.println("Error in outputoverallLCL: unknown label name: " + label);
            System.exit(0);
          }
          int labelcode = labelspace.get(label);
          positive[labelcode] = 1;
          
          double innerproduct = 0;
          for(long code: V.keySet()){
            innerproduct += V.get(code) * B[labelcode].get(code);
          }
          double p = sigmoid(innerproduct);
          ret += Math.log(p);
        }
        
        for(int labelcode = 0; labelcode < labelsize; ++labelcode){
          if(positive[labelcode] == 1)continue;
          double innerproduct = 0;
          for(long code: V.keySet()){
            innerproduct += V.get(code) * B[labelcode].get(code);
          }
          double p = sigmoid(innerproduct);
          ret += Math.log(1 - p);
        }
      }
      br.close();
      in.close();
      fstream.close();
      System.out.println("Overall likelihood after round " + (int)(Math.floor(k / trainsize)) + ": " + ret);
    }
    catch(Exception e) {// Catch exception if any
      System.err.println("Error in outputoverallLCL: " + e.getMessage());
    }
  }

  
  
  
  
  private static void updateWeight() throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String s, labels, content;
    String[] labeltokens;
    Vector<String> contenttokens;
    Hashtable<Long, Integer> V = new Hashtable<Long, Integer>();
    int splitposition;
    
    
    int iteration = 0;
    while ((s = in.readLine()) != null && s.length() != 0) {

      // clear the hash trick array for each example(here I use hashtable)
      V.clear();
      k += 1;
      if(verbose)System.out.print(k + "\r");
      if(k % trainsize == 1){
        if(verbose && iteration > 0){
          outputoverallLCL(k);
        }
        ++iteration;
      }
      double lambda = eta/ (iteration * iteration);
      splitposition = s.indexOf("\t");
      labels = s.substring(0, splitposition);
      content = s.substring(splitposition + 1, s.length());
      labeltokens = labels.split(",");
      contenttokens = tokenizeDoc(content);

      // hash kernel trick
      for (String token : contenttokens) {
        long Code = hashcode(token);
        if (V.containsKey(Code)) {
          V.put(Code, V.get(Code) + 1);
        } else {
          V.put(Code, 1);
        }
      }

      int positive[] = new int[labelsize];
      // deal with positive labels first
      for (String label : labeltokens) {
        if (!labelspace.containsKey(label)) {
          System.out.println("Error in updateWeight: unknown label name: " + label);
          System.exit(0);
        }
        int labelcode = labelspace.get(label);
        positive[labelcode] = 1;

        double innerprod = 0;
        for (long code : V.keySet()) {
          double newvalue = 0;
          if (!B[labelcode].containsKey(code)) {
            B[labelcode].put(code, 0.0);
          } else {
            //penalty part
            newvalue = B[labelcode].get(code)
                    * Math.pow(1 - 2 * lambda * mu, k - A[labelcode].get(code));
            B[labelcode].put(code, newvalue);
          }
          innerprod += V.get(code) * newvalue;
          A[labelcode].put(code, k);
        }

        double p = sigmoid(innerprod);

        // update the weight
        for (long code : V.keySet()) {
          B[labelcode].put(code, B[labelcode].get(code) + (lambda * (1 - p) * V.get(code)));
        }
      }

      // deal with negative labels then
      for (int labelcode = 0; labelcode < labelsize; labelcode ++) {
        if(positive[labelcode] == 1)continue;
        double innerprod = 0;
        for (long code : V.keySet()) {
          double newvalue = 0;
          if (!B[labelcode].containsKey(code)) {
            B[labelcode].put(code, 0.0);
          } else {
            //penalty part
            newvalue = B[labelcode].get(code)
                    * Math.pow((1 - 2 * lambda * mu), k - A[labelcode].get(code));
            B[labelcode].put(code, newvalue);
          }
          innerprod += V.get(code) * newvalue;
          A[labelcode].put(code, k);
        }

        double p = sigmoid(innerprod);

        // update the weight
        for (long code : V.keySet()) {
          B[labelcode].put(code, B[labelcode].get(code) + (lambda * -p * V.get(code)));
        }
      }
    }
    
    //after going through all examples, do the decayed penalty for all weight
    penalWeight(k);
    if(verbose && iteration > 0){
      outputoverallLCL(k);
    }
  }
  
  
  
  private static void test(){
    
    long correct[] = new long[14];
    
    try {
      FileInputStream fstream = new FileInputStream(inputtestfile);
      // Get the object of DataInputStream
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));

      String s, labels, content;
      String[] labeltokens;
      Vector<String> contenttokens;
      Hashtable<Long, Integer> V = new Hashtable<Long, Integer>();
      int splitposition;

      long total = 0;
      while ((s = br.readLine()) != null && s.length() != 0) {
        
        ++total;
        splitposition = s.indexOf("\t");
        labels = s.substring(0, splitposition);
        content = s.substring(splitposition + 1, s.length());
        labeltokens = labels.split(",");
        contenttokens = tokenizeDoc(content);
        
        V.clear();
        for (String token : contenttokens) {
          long Code = hashcode(token);
          if (V.containsKey(Code)) {
            V.put(Code, V.get(Code) + 1);
          } else {
            V.put(Code, 1);
          }
        }
        
        int positive[] = new int[labelsize];
        // deal with positive labels first
        for (String label : labeltokens) {
          if (!labelspace.containsKey(label)) {
            System.out.println("Error in outputoverallLCL: unknown label name: " + label);
            System.exit(0);
          }
          int labelcode = labelspace.get(label);
          positive[labelcode] = 1;
          
          double innerproduct = 0;
          for(long code: V.keySet()){
            if(!B[labelcode].containsKey(code))continue;
            innerproduct += V.get(code) * B[labelcode].get(code);
          }
          double p = sigmoid(innerproduct);
          if(p >= 0.5) {
            ++correct[labelcode];
          }
        }
       for (int labelcode = 0; labelcode < labelsize; ++labelcode){
         if(positive[labelcode] == 1)continue;
         double innerproduct = 0;
         for(long code: V.keySet()){
           if(!B[labelcode].containsKey(code))continue;
           innerproduct += V.get(code) * B[labelcode].get(code);
         }
         double p = sigmoid(innerproduct);
         if(p < 0.5) ++correct[labelcode];
       }           
      }
      System.out.print("\n");
      double avg = 0;
      for(String label: labelspace.keySet()){
        
        long thiscorrect = correct[labelspace.get(label)];
        System.out.println("For label: " + "(" + label + ")" + ", Percent correct: " + thiscorrect + "/" + total + "=" + (thiscorrect / (double)total * 100) + "%");
        avg += thiscorrect / (double)total / labelsize;
      }
      
      System.out.println("Average percent correct: " + avg * 100 + "%");
      
    }
    catch(Exception e) {// Catch exception if any
      e.printStackTrace();
      System.err.println("Error in test(): " + e.getMessage());
    }
  }
  

  public static void main(String args[]) throws IOException {
    if (args.length == 8 && args[0].equals("-n") && args[2].equals("-u") && args[4].equals("-N") && args[6].equals("-test")) {
      trainsize = Long.parseLong(args[1]);
      mu = Double.parseDouble(args[3]);
      N = Long.parseLong(args[5]);
      inputtestfile = args[7];
    }
    else if(args.length == 10 && args[0].equals("-n") && args[2].equals("-u") && args[4].equals("-N") && args[6].equals("-train") && args[8].equals("-test")){
      trainsize = Long.parseLong(args[1]);
      mu = Double.parseDouble(args[3]);
      N = Long.parseLong(args[5]);
      inputtrainfile = args[7];
      inputtestfile = args[9];
      verbose = true;

    }
    else{
      System.out.println("usage: -n <size of the train_set> -u <mu> -N <dictionary size> -train <trainning file> -test <testing file>");
      System.out.println("The -train argument only needed when you want to output the overall likelihood function for each iteration");
      System.exit(0);
    }
    Init();
    updateWeight();
    //outputWeight();
    test();
  }

}
