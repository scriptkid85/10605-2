import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class MyTrainertrain {

  private static Hashtable<Vector<String>, Integer> counters = new Hashtable<Vector<String>, Integer>();

  private static int hashtablemaxsize = 100000;
  
  private static int totalcount = 0;
  
  private static int numberofinstance = 0;
  
  public MyTrainertrain() {
    counters = new Hashtable<Vector<String>, Integer>();
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

  private static void outputCounter() throws IOException {
    if(counters.size() > 0){
      int value;
      BufferedWriter outputcounter = new BufferedWriter(new OutputStreamWriter(System.out));
    
      for (Iterator<Vector<String>> it = counters.keySet().iterator(); it.hasNext();) {
        Vector<String> key = (Vector<String>) it.next();
        value = counters.get(key);
        if(key.size() == 1){
          outputcounter.write(key.get(0) + "\t" + value + "\n");
          outputcounter.flush();
        }
        else {
          outputcounter.write(key.get(1) + " " + key.get(0) + "\t" + value + "\n");
          outputcounter.flush();
        }
      }
    }
  }
  
  private static void output_resetHT(){
    if(counters.size() > hashtablemaxsize){
      try {
        outputCounter();
      } catch (IOException e) {
        System.out.println("Output counter error");
        e.printStackTrace();
      }
      counters.clear();
    }
  }
  
/*
  private static void updateCounter() throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String s;
    Vector<String> tokens;

    while ((s = in.readLine()) != null && s.length() != 0) {
      tokens = tokenizeDoc(s);
      for (String token : tokens) {
        if (counters.containsKey(token))
          counters.put(token, counters.get(token) + 1);
        else
          counters.put(token, 1);
      }
    }
  }
*/
  
  private static void updateCounter() throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String s, labels, content;
    String[] labeltokens;
    Vector<String> contenttokens, tempkey;
    int splitposition;

    while ((s = in.readLine()) != null && s.length() != 0) {
      splitposition = s.indexOf("\t");
      labels = s.substring(0, splitposition);
      content = s.substring(splitposition + 1, s.length());
      labeltokens = labels.split(",");
      contenttokens = tokenizeDoc(content);
      
      for(String label : labeltokens){
          
        //Y = ANY
        tempkey = new Vector<String>();
        tempkey.add("*");
        if(counters.containsKey(tempkey))
          counters.put(tempkey, counters.get(tempkey) + 1);
        else {
          output_resetHT();
          counters.put(tempkey, 1);
        }

        //Y = label
        tempkey = new Vector<String>();
        tempkey.add(label);
        if(counters.containsKey(tempkey))
          counters.put(tempkey, counters.get(tempkey) + 1);
        else {
          output_resetHT();
          counters.put(tempkey, 1);
        }
        
        for(String token: contenttokens){
          
          tempkey = new Vector<String>();
          tempkey.add(label);
          tempkey.add(token);
          if(counters.containsKey(tempkey))
            counters.put(tempkey, counters.get(tempkey) + 1);
          else {
            output_resetHT();
            counters.put(tempkey, 1);
          }
          
          tempkey = new Vector<String>();
          tempkey.add(label);
          tempkey.add("*");
          if(counters.containsKey(tempkey))
            counters.put(tempkey, counters.get(tempkey) + 1);
          else {
            output_resetHT();
            counters.put(tempkey, 1);
          }
        }
       totalcount += contenttokens.size();
      }
    }
  }



  public static void main(String args[]) throws IOException {
    totalcount = 0;
    numberofinstance = 0;
    updateCounter();
    outputCounter();
  }

}
