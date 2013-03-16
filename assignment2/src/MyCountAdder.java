import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;



public class MyCountAdder {
  
  private static Hashtable<Vector<String>, Integer> traincounters = new Hashtable<Vector<String>, Integer>();

  private static int vocabularysize;
  
  private static int labelspace;
  
  
  private static void aggregateCounter() throws IOException {
    try{
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    BufferedWriter outputcounter = new BufferedWriter(new OutputStreamWriter(System.out));
    String s;
    String[] tokens, keys;
    String prelabel = "";
    String preword = "";
    int sumForPreviousKey = 0;
    s = in.readLine();
    String prekey = s.split("\t")[0]; //key is word&label
    sumForPreviousKey = Integer.parseInt(s.split("\t")[1]);
    
    while ((s = in.readLine()) != null && s.length() != 0) {
      tokens = s.split("\t");
      
      if(tokens[0].equals(prekey)){
        sumForPreviousKey += Integer.parseInt(tokens[1]);
      }
      else{
        keys = tokens[0].split(" ");
        if(keys.length == 2 && keys[0].equals("*")){
          if(!keys[1].equals(prelabel)){
            prelabel = keys[1];
            labelspace ++;
          }
        }
        else if(keys.length == 2 && !keys[0].equals(preword)){
          vocabularysize ++;
          preword = keys[0];
        }
        outputcounter.write(prekey + "\t" + sumForPreviousKey + "\n");
        outputcounter.flush();
        prekey = tokens[0];
        sumForPreviousKey = Integer.parseInt(tokens[1]);
      }
    }
    
    outputcounter.write(prekey + "\t" + sumForPreviousKey + "\n");
    outputcounter.flush();
    
    outputcounter.write("vocabsize" + "\t" + vocabularysize + "\n");
    outputcounter.flush();
    
    outputcounter.write("labelspace" + "\t" + labelspace + "\n");
    outputcounter.flush();

  }catch(Exception e) {// Catch exception if any
    System.err.println("Error in aggregateCounter: " + e.getMessage());
  }
 }
  
  
  private static void aggregateFromFile(String inputhashtable){
    try {
      // Open the file that is the first
      // command line parameter
      FileInputStream fstream = new FileInputStream(inputhashtable);
      // Get the object of DataInputStream
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      BufferedWriter outputcounter = new BufferedWriter(new OutputStreamWriter(System.out));

      String s;
      String[] tokens, keys;
      String prelabel = "";
      String preword = "";
      int sumForPreviousKey = 0;
      s = br.readLine();
      String prekey = s.split("\t")[0]; //key is word&label
      sumForPreviousKey = Integer.parseInt(s.split("\t")[1]);

      while ((s = br.readLine()) != null && s.length() != 0) {
        tokens = s.split("\t");
        
        if(tokens[0].equals(prekey)){
          sumForPreviousKey += Integer.parseInt(tokens[1]);
        }
        else{
          keys = tokens[0].split(" ");
          if(keys.length == 2 && keys[0].equals("*")){
            if(!keys[1].equals(prelabel)){
              prelabel = keys[1];
              labelspace ++;
            }
          }
          else if(keys.length == 2 && !keys[0].equals(preword)){
            vocabularysize ++;
            preword = keys[0];
          }
          outputcounter.write(prekey + "\t" + sumForPreviousKey + "\n");
          outputcounter.flush();
          prekey = tokens[0];
          sumForPreviousKey = Integer.parseInt(tokens[1]);
        }
      }
      
      outputcounter.write(prekey + "\t" + sumForPreviousKey + "\n");
      outputcounter.flush();
      
      outputcounter.write("vocabsize" + "\t" + vocabularysize + "\n");
      outputcounter.flush();
      
      outputcounter.write("labelspace" + "\t" + labelspace + "\n");
      outputcounter.flush();
    }
    catch(Exception e) {// Catch exception if any
      System.err.println("Error in aggregateFromFile: " + e.getMessage());
    }
  }
  
  
  public static void main(String args[]){
    vocabularysize = 0;
    labelspace = 0;
    try {
      aggregateCounter();
    } catch (IOException e) {
      e.printStackTrace();
    }
//    aggregateFromFile("raw");
  }
}
