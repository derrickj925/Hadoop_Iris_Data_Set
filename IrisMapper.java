
package Iris;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.lang.*;

/*
Derrick Jann

Preconditions: A tab deliminted file with the format SepalLength, SepalWidth, PetalLength, PetalWidth, ID
Mapper Input: LongWritable, Text, Text, Text
Mapper Output: Text, Text
Funtion: Parse input file, and then have a output to the mapper with the format (ID, "sepLength_sepWidth_petLength_petWidth")


*/


public class IrisMapper  extends Mapper <LongWritable,Text,Text,Text> {
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      
     
      String valuen= value.toString();
      String[] tokens = valuen.split("\\s+");  
   
      String sepalLength = tokens[0];
     
      String sepalWidth = tokens[1];
      String petalLength = tokens[2];
      String petalWidth = tokens[3];
      String flowerId = tokens[4];
     
      context.write(new Text(flowerId), new Text(sepalLength + "_" + sepalWidth + "_" + petalLength + "_" + petalWidth));
   
   }
}
