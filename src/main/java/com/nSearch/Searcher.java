
package com.nSearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import java.net.HttpURLConnection;
import java.net.URL;

public class Searcher extends Configured implements Tool {

    private final String URL_LOCAL = "http://localhost:5000/results";
    private final String URL_DEV =   "http://yinyang.herokuapp.com/results";


    public static class MapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text resultId = new Text();
        private Text keyText = new Text();

        private static String searchKey;

        public void configure(JobConf job) {
            searchKey = job.get("keyword");
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String id;

            StringTokenizer itr = new StringTokenizer(line,",");

            id = itr.nextToken();

            if((line.toLowerCase()).contains(searchKey.toLowerCase())) {
                resultId.set(id);
                keyText.set(searchKey);
                output.collect(keyText,resultId);
            }
        }
    }

    public static class ReducerClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String result = "";
            Text resultT = new Text();
            boolean first = true;
            while (values.hasNext()) {
                if(first) {
                    result += values.next();
                    first = false;
                } else {
                    result += "," + values.next();
                }
            }

            resultT.set(result);

            output.collect(key, resultT);
        }
    }

    private static int printUsage() {
        System.out.println("nSearch [-m <maps>] [-r <reduces>] <input> <output> <keyword>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    private void sendResult(String result, String userId, String selectedUrl) throws IOException {

        String postData = "{ \"data\": \"" + result + "\" , \"id\": \"" + userId + "\" }";

        System.out.println(String.format("Post data: %s",postData));

        URL url = new URL(selectedUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Content-Length",  String.valueOf(postData.length()));

        // Write data
        OutputStream os = connection.getOutputStream();
        os.write(postData.getBytes());

        // Read response
        StringBuilder responseSB = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

        String line;
        while ( (line = br.readLine()) != null)
            responseSB.append(line);

        // Close streams
        br.close();
        os.close();

        System.out.println("Envio concluido!");
    }

    private String retrieveResult (String outputPath) throws IOException, InterruptedException {
        String result = "";
        String command = String.format("hdfs dfs -cat %s/part-* ", outputPath);

        Runtime rt = Runtime.getRuntime();
        Process pr = rt.exec(command);

        BufferedReader reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));

        String line;
        while ((line = reader.readLine()) != null)
            result += line;

        pr.waitFor();

        String[] sub = result.split("\t");

        result = sub[sub.length - 1];

        return result;
    }

    public int run(String[] args) throws Exception {

        final long startTime, endTime;
        String results, selectedUrl;

        //////////// Arguments ///////////
        String outputArg, localArg, inputArg, userIdArg, keywordArg;

        startTime = System.currentTimeMillis();

        JobConf conf = new JobConf(getConf(), Searcher.class);
        conf.setJobName("nSearch");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
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

        //////////// Retrieving arguments ///////////
        if (other_args.size() != 5) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 5.");
            return printUsage();
        }

        inputArg   = other_args.get(0);
        outputArg  = other_args.get(1);
        keywordArg = other_args.get(2).replaceAll("%"," ");
        userIdArg  = other_args.get(3);
        localArg   = other_args.get(4);

        if((new String("dev")).equals(localArg))
        {
            selectedUrl = URL_DEV;
        } else {
            selectedUrl = URL_LOCAL;
        }
        //////////////////////////////////////////

        Path outputPath = new Path(outputArg);
        FileInputFormat.setInputPaths(conf, inputArg);
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.set("keyword",keywordArg);

        JobClient.runJob(conf);

        results = retrieveResult(outputArg);

        System.out.println(String.format("Url selecionada: %s", selectedUrl));
        System.out.println(String.format("User id: %s", userIdArg));
        System.out.println(String.format("Palavra chave: %s", keywordArg));
        System.out.println(String.format("Resultado final: %s", results));

        try {
            System.out.println("Enviando a mensagem");
            sendResult(results, userIdArg, selectedUrl);
        } catch (Exception e) {
            System.out.println("Erro ao enviar a mensagem!");

            System.out.println(e.getMessage());
        } finally {
            outputPath.getFileSystem(conf).delete(outputPath,true);
        }

        endTime = System.currentTimeMillis();

        System.out.println("Finalizando");
        System.out.println("Total execution time: " + (endTime - startTime)/1000 + " seg" );

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Searcher(), args);
        System.exit(res);
    }

}
