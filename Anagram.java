package anagram;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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

    public static class AnagramMakerMapper extends Mapper<Object, Text, Text, Text> {
    	
    	private String input;
    	private boolean caseSensitive = false;
    	private Set<String> patternsToSkip = new HashSet<String>();
    	
    	protected void setup(Mapper.Context context)
    	        throws IOException,
    	        InterruptedException {
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
    	        		patternsToSkip.add(arr[i]);
    	        	}
        			
    	        }
    	      } catch (IOException ioe) {
    	        System.err.println("Caught exception while parsing the cached file '"
    	            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
    	      }
    	    }
    	
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	
                String word = itr.nextToken();
                if(patternsToSkip.contains(word)) {
                	continue;
                } else {
                	char[] arr = word.toCharArray();
                    Arrays.sort(arr);
                    String wordKey = new String(arr);
                    context.write(new Text(wordKey), new Text(word));
                }
            	
                
            }
        }
    }
    

    public static class AnagramAggregatorReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String anagram = null;
            for (Text val : values) {
                if (anagram == null) {
                    anagram = val.toString();
                } else {
                    anagram = anagram + ',' + val.toString();
                }
            }
            HashSet<String> test=new HashSet<String>(Arrays.asList(anagram.split(",")));
            int size = test.size();
            if(size > 1) {
            	String s = test.toString();
                context.write(key, new Text(s));
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram");
        for (int i = 0; i < args.length; i += 1) {
            if ("-skip".equals(args[i])) {
              job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
              i += 1;
              job.addCacheFile(new Path(args[i]).toUri());
            }
          }
        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramMakerMapper.class);
        job.setReducerClass(AnagramAggregatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
