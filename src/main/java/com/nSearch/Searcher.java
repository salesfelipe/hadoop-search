
package com.nSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;


public class Searcher extends Configured implements Tool {

    public static class MapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static String searchKey;

        public void configure(JobConf job) {
            searchKey = job.get("name");
        }

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            String line = value.toString();
            String id, conteudo;

            StringTokenizer itr = new StringTokenizer(line,",");

            id = itr.nextToken();
            conteudo = itr.nextToken();

            if((conteudo.toLowerCase()).contains(searchKey.toLowerCase())) {
                word.set(id);
                output.collect(word, one);
            }
        }
    }

    public static class ReducerClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    private static int printUsage() {
        System.out.println("nSearch [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }


    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), Searcher.class);
        conf.setJobName("nSearch");

        // the keys are words (strings)
        conf.setOutputKeyClass(Text.class);
        // the values are counts (ints)
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MapperClass.class);
        conf.setCombinerClass(ReducerClass.class);
        conf.setReducerClass(ReducerClass.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }

        // Make sure there are exactly 2 parameters left.
//        if (other_args.size() != 2) {
//            System.out.println("ERROR: Wrong number of parameters: " +
//                    other_args.size() + " instead of 2.");
//            return printUsage();
//        }

        FileInputFormat.setInputPaths(conf, other_args.get(0));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        conf.set("name",other_args.get(2));

        JobClient.runJob(conf);
        return 0;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Searcher(), args);
        System.exit(res);
    }

}
