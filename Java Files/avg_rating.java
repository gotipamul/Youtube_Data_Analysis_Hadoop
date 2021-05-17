import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class avg_rating {

    public static class Map extends Mapper<LongWritable, Text, Text,
FloatWritable> {

       private Text video_name = new Text();
       private  FloatWritable rating = new FloatWritable();
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split(",");

          if(str.length > 9){
                video_name.set(str[0]);
                if(str[8].matches("\\d+.+")){ //this regular expression
                float f=Float.parseFloat(str[8]); //typecasting string to float
                rating.set(f);
                }
          }
                context.write(video_name, rating);
      }

    }

/*  public static class IntSumReducer
          extends Reducer<Text,FloatWritable,Text,FloatWritable> {
      private FloatWritable result = new FloatWritable();
  
      public void reduce(Text key, Iterable<FloatWritable> values,
                         Context context) 
throws IOException, InterruptedException {
          float sum = 0;
          for (FloatWritable val : values) {
              sum += val.get();
          }
          result.set(sum);
          context.write(key, result);
      }
  }
  
  public static class Reduce
          extends Reducer<Text,FloatWritable,Text,FloatWritable> {
      private FloatWritable result = new FloatWritable();
      Float average = 0f;
      Float count = 0f;
      int sum = 0;
      public void reduce(Text key, Iterable<FloatWritable> values,
                         Context context
      ) throws IOException, InterruptedException {
  
          Text sumText = new Text("average");
          for (FloatWritable val : values) {
              sum += val.get();
  
          }
          count += 1;
          average = sum/count;
          result.set(average);
          context.write(sumText, result);
      }
  }*/

  public static class Reduce extends Reducer<Text, FloatWritable,
Text, FloatWritable> {
           FloatWritable rating = new FloatWritable();
           int count=0;
           float avg;
       public void reduce(Text key, Iterable<FloatWritable> values,
Context context)
         throws IOException, InterruptedException {

            float sum = 0;
           Text sumtext=new Text("average");
           for (FloatWritable val : values) {
               sum += val.get();
               }
               count+=1;
            avg=sum/count;
           rating.set(avg);
           context.write(sumtext,rating);
       }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

           @SuppressWarnings("deprecation")
                Job job = new Job(conf, "avg_rating");
           job.setJarByClass(avg_rating.class);
           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(FloatWritable.class);
      //job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(FloatWritable.class);

       job.setMapperClass(Map.class);
        job.setMapperClass(IntSumReducer.class);
       job.setReducerClass(Reduce.class);

       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
       job.waitForCompletion(true);
    }

  }
