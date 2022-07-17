import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageGrades {



    public static class TextMapper
  
        extends Mapper<Object, Text, Text, IntWritable> {
  
  
  
      private Text module = new Text();
  
      private FloatWritable grade = new FloatWritable();
  
  
  
      public void map(Object key, Text value, Context context)
  
        throws IOException, InterruptedException
  
      {
  
        String[] lines = value.split("\\r?\\n");
  
        for (int i = 1; i < lines.length; i++) {
  
          row = lines[i];
  
          String[] fields = row.split(",");
  
          module.set(fields[4]);
  
          grade.set(fields[5]);
  
          context.write(module, grade);
  
        }
  
      }
  
    }

}

public static class GradeReducer

    extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();



    public void reduce(Text key, Iterable<IntWritable> values, Context context)

      throws IOException, InterruptedException

    {

      int count = values.length;

      float sum = 0;

      for (IntWritable val : values) {

        sum += val.get();

      }

      result.set(sum/count);

      context.write(key, result);

    }

  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(AverageGrades.class);

    job.setMapperClass(TextMapper.class);

    job.setCombinerClass(GradeReducer.class);

    job.setReducerClass(GradeReducer.class);

    job.setOutputKeyClass(Text.class);

    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}
