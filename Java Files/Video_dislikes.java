import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class Video_dislikes {

    public static class Map extends Mapper<LongWritable, Text, Text,
IntWritable> {

       private Text video = new Text();
       private IntWritable dislikes = new IntWritable();
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split(",");
            int max=0;
          if(str.length > 10){
                video.set(str[0]);
                if(str[8].matches("\\d+")){  //this regular expression checks whether the data contain only integer values
                        int i=Integer.parseInt(str[8]); //type casting string to an int
                        dislikes.set(i);
                }
          }
                                 
                 context.write(video, dislikes);
      
      }

    }

    public static class Reduce extends Reducer<Text, IntWritable,
Text, IntWritable> {
 int max=0;
Text maxWord=new Text();


       public void reduce(Text key, Iterable<IntWritable> values,
Context context)
         throws IOException, InterruptedException {
           int sum = 0;
  
          
           for (IntWritable val : values) {

               sum += val.get();
           }
       if(sum>max)
          {
             max=sum;
             maxWord.set(key);
          }
        }
          protected void cleanup(Context context) throws IOException, InterruptedException{
     

           context.write(maxWord, new IntWritable(max));
       }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

           @SuppressWarnings("deprecation")
                Job job = new Job(conf, "video_dislikes");
           job.setJarByClass(Video_dislikes.class);

           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(IntWritable.class);
      //job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       job.setMapperClass(Map.class);
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
