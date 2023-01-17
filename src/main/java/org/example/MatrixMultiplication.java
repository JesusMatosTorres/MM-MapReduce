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
import java.util.HashMap;
import java.util.Map;

public class MatrixMultiplication {

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
        job.waitForCompletion(true);
    }

    public static class MatrixMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            MatrixElement element = MatrixElement.fromText(value);

            for (int i = 1; i <= element.getNumCols(); i++) {
                outKey.set(i);
                outValue.set(element.getRow() + "," + element.getValue());
                context.write(outKey, outValue);
            }
        }
    }

    public static class MatrixReducer
            extends Reducer<IntWritable, Text, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private Text outKey = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            Map<Integer, Integer> leftMatrix = new HashMap<>();
            Map<Integer, Integer> rightMatrix = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                int row = Integer.parseInt(parts[0]);
                int value = Integer.parseInt(parts[1]);

                if (row <= key.get()) {
                    leftMatrix.put(row, value);
                } else {
                    rightMatrix.put(row - key.get(), value);
                }
            }

            for (Map.Entry<Integer, Integer> left : leftMatrix.entrySet()) {
                for (Map.Entry<Integer, Integer> right : rightMatrix.entrySet()) {
                    if (left.getKey().equals(right.getKey())) {
                        sum += left.getValue() * right.getValue();
                    }
                }
            }
            outKey.set(key.get() + "," + (key.get() + rightMatrix.size()));
            result.set(sum);
            context.write(outKey, result);
        }
    }
}

