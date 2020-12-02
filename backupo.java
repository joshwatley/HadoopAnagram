import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Anagram {

    static Collection<Text> anagrams = new HashSet<Text>();

    public static class Mapper extends Mapper<Object, Text, Text, Text> { 
        // this is the class for mapping the data from its original format into its anagram key pair
    
        private String input;
        private boolean caseSensitive = false;
        private Set<String> skipwords = new HashSet<String>();
    
        protected void setup(Mapper.Context context)
                throws IOException,
                InterruptedException {
                    // this will allow me to parse the skip file allowing me to not include these
                    // words in the program, this will save the amount of work needed as well
              if (context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
              } else {
                this.input = context.getInputSplit().toString();
              }
              Configuration config = context.getConfiguration();
              this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
              if (config.getBoolean("wordcount.skip.patterns", false)) {
                URI[] localPaths = context.getCacheFiles();
                parseSkipFile(localPaths[0]);
              }
            }
    
        private void parseSkipFile(URI patternsURI) {
              try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                        String[] arr = pattern.split(",");
                        for (int i =0 ;i< arr.length; i++) {
                                skipwords.add(arr[i]); // add skip words to set
                        }
        
                }
              } catch (IOException ioe) {
                System.err.println("Error Parsing file");
              }
            }
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException { // mapper function
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}", "").toLowerCase());
            while (itr.hasMoreTokens()) { // check for more words
            
                String word = itr.nextToken();
                if(skipwords.contains(word)) {
                        continue; // this will skip words if contained in skipword file
                } 
                else if(word.matches(".*\\d+.*")){
                        continue;
                }
                else {
                    char[] arr = word.toCharArray();
                    Arrays.sort(arr);
                    String anagramKey = new String(arr);
                    context.write(new Text(anagramKey), new Text(word)); // complete the mapping of anagram key to the word
                }
            
                
            }
        }
    }
    

    public static class Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String anagram = null;
            for (Text mapped : values) {
                if (anagram == null) {
                    anagram = mapped.toString();
                } else {
                    anagram = anagram + ',' + mapped.toString();
                }
            }

            HashSet<String> tempAnagramList=new HashSet<String>(Arrays.asList(anagram.split(",")));

            // additional feature, changing the order of the anagramlist
            //Collections.sort(tempAnagramList)
            int size = tempAnagramList.size();
            if(size > 1) {
                String finalAnagrams = tempAnagramList.toString();
                context.write(key, new Text(finalAnagrams));
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram");



 		// allow an argument for the skip file location
        for (int i = 0; i < args.length; i += 1) {
            if ("-skip".equals(args[i])) {
              job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
              i += 1;
              job.addCacheFile(new Path(args[i]).toUri());
            }
          }


        // configuration setup
        job.setJarByClass(Anagram.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }