package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class MatrixMultiplication {

    public static class MatrixMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            MatrixElement element = MatrixElement.fromText(value);

            for (int i = 1; i <= element.getNumCols(); i++) {
                int row = element.getRow();
                int col = i;
                int subRow = (row - 1) / 4 + 1;
                int subCol = (col - 1) / 4 + 1;
                String subMatrixKey = subRow + "," + subCol;
                outKey.set(subMatrixKey);
                outValue.set(element.getCol() + "," + element.getValue(col));
                context.write(outKey, outValue);
            }
        }
    }

    public static class MatrixReducer
            extends Reducer<Text,Text,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                int col = Integer.parseInt(parts[0]);
                int value = Integer.parseInt(parts[1]);
                MatrixElement element = MatrixElement.fromText(val);

                sum += value * element.getValue(col);
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/MatrixInput"));
        FileOutputFormat.setOutputPath(job, new Path("/MatrixOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

