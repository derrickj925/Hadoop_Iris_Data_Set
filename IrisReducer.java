package Iris;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import java.io.IOException;


public class IrisReducer  extends Reducer <Text,Text,Text,Text> {
   String[] tempString;
   float count=0;
   float tempSepalLength, tempSepalWidth, tempPetalLength, tempPetalWidth;
   float totalSepalLength, totalSepalWidth, totalPetalLength,  totalPetalWidth;
   float minSepalLength, maxSepalLength, meanSepalLength, minSepalWidth, maxSepalWidth, meanSepalWidth, minPetalLength, maxPetalLength, meanPetalLength, minPetalWidth, maxPetalWidth, meanPetalWidth;

   /*
  Derrick Jann

  Preconditions: A mapper that produces the output (Text key, Text(sepLength_sepWidth_petLength_petWidth))
  Reduce Input: Text, Text, Context
  Mapper Output: Text, Text
  Funtion: Calculate the mean\min\max PetalLength, PetalWidth, SepalLength, SepalWidth for the species)


  */

   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

       minSepalLength = minPetalLength = minSepalWidth = minPetalWidth = Float.MAX_VALUE;
       maxSepalLength = maxPetalLength = maxSepalWidth = maxPetalWidth = Float.MIN_VALUE;
    

      for(Text value: values) {
         // String split() method to split value and assign to tempString
        tempString = value.toString().split("_");

         // convert Strings to Floats
        tempSepalLength= Float.parseFloat(tempString[0]); /
        tempSepalWidth = Float.parseFloat(tempString[1]);
        tempPetalLength= Float.parseFloat(tempString[2]);
        tempPetalWidth= Float.parseFloat(tempString[3]);

         // determine if you have min/max sepal/petal length/widths and assign to min/max sepal/petal lenght/widths accordingly
        if(tempPetalLength>maxPetalLength)
              maxPetalLength=tempPetalLength;
        if(tempPetalLength<minPetalLength)
              minPetalLength=tempPetalLength;

        if(tempPetalWidth>maxPetalWidth)
              maxPetalWidth=tempPetalWidth;
        if(tempPetalWidth<minPetalWidth)
              minPetalWidth=tempPetalWidth;

        if(tempSepalLength>maxSepalLength)
              maxSepalLength= tempSepalLength;
        if(tempSepalLength<minSepalLength)
              minSepalLength= tempSepalLength;

        if(tempSepalWidth>maxSepalWidth)
              maxSepalWidth= tempSepalWidth;
        if(tempSepalWidth<minSepalWidth)
              minSepalWidth= tempSepalWidth;


         // calculate running totals for sepal/petal length/widths for use in calculation of means
        totalSepalLength+=tempSepalLength;
        totalSepalWidth+=tempSepalWidth;
        totalPetalLength+=tempPetalLength;
        totalPetalWidth+=tempPetalWidth;


         //increment counter for use in calculation of means
          count++;

      } 
     
      //calculate mean sepal/petal length/width 
      meanSepalLength= totalSepalLength/count;
      meanSepalWidth= totalSepalWidth/count;
      meanPetalLength= totalPetalLength/count;
      meanPetalWidth= totalPetalWidth/count;

      //convert to string so the output of the reducer is a string
      String minSepalLengthString= Float.toString(minSepalLength);
      String maxSepalLengthString= Float.toString(maxSepalLength);
      String meanSepalLengthString= Float.toString(meanSepalLength);

      String minSepalWidthString= Float.toString(minSepalWidth);
      String maxSepalWidthString= Float.toString(maxSepalWidth);
      String meanSepalWidthString= Float.toString(meanSepalWidth);

      String minPetalLengthString= Float.toString(minPetalLength);
      String maxPetalLengthString= Float.toString(maxPetalLength);
      String meanPetalLengthString= Float.toString(meanPetalLength);

      String minPetalWidthString= Float.toString(minPetalWidth);
      String maxPetalWidthString= Float.toString(maxPetalWidth);
      String meanPetalWidthString= Float.toString(meanPetalWidth);

      //format the results
      String output =  minSepalWidthString+"\t"+maxSepalWidthString+"\t"+meanSepalWidthString+"\t"+ minSepalLengthString+"\t"+maxSepalLengthString+"\t"+meanSepalLengthString+"\t"+ minPetalWidthString+"\t"+maxPetalWidthString+"\t"+meanPetalWidthString+ "\t"+ minPetalLengthString+"\t"+maxPetalLengthString+"\t"+meanPetalLengthString; 

      //emit output to context
      context.write(key, new Text(output));
      //output: (Species, "minSepalWidth  maxSepalWidth  meanSepalWidth  minSepalLength  maxSepalLength  meanSepalLength........")

   }
}
