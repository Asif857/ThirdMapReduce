package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class Main {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String currKey = itr.nextToken();
                String val = itr.nextToken();
                word.set(currKey.replaceAll(",", " "));
                if (!currKey.equals("*")) {
                    context.write(word, new Text(val));
                }
            }
        }
    }

    public static class ParametersReducer
            extends Reducer<Text, Text, Text, FloatWritable> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float zeroN = 1,oneN = 1,zeroT = 1,oneT = 1;
            float N = Integer.parseInt(context.getConfiguration().get("N"));
            for(Text value : values){
                String valueString = value.toString();
                switch(valueString.substring(0,2)){
                    case "0N":
                        zeroN = Integer.parseInt(valueString.substring(3));
                        break;
                    case "1N":
                        oneN = Integer.parseInt(valueString.substring(3));
                        break;
                    case "0T":
                        zeroT = Integer.parseInt(valueString.substring(3));
                        break;
                    case "1T":
                        oneT = Integer.parseInt(valueString.substring(3));
                        break;
                }
            }
            float ans = (oneT + zeroT) / (N * (oneN + zeroN));
            context.write(key,new FloatWritable(ans));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("N", bringNValue(new Path(args[0] + "/part-r-00000")));
        Job job = Job.getInstance(conf, "EMR3");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(ParametersReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static String bringNValue(Path path) throws IOException {
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
        try {
            if ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\t");
                String value = parts[1];
                return value;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            reader.close();
        }
        return "";
    }
}