package io.markov;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.map.HMapStIW;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfStrings;

public class Dictogram extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(Dictogram.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, Text> {
    private static final PairOfStrings BIGRAM = new PairOfStrings();
    private static final Text VAL = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());

      if (tokens.size() < 3) return;
      BIGRAM.set("*START*", "*START*");
      VAL.set(tokens.get(0) + " " + tokens.get(1));
      context.write(BIGRAM, VAL);

      for (int i = 2; i < tokens.size(); i++) {
        BIGRAM.set(tokens.get(i - 2), tokens.get(i - 1));
        VAL.set(tokens.get(i));
        context.write(BIGRAM, VAL);
      }

      BIGRAM.set("*END*", "*END*");
      VAL.set(tokens.get(tokens.size() - 2) + " " + tokens.get(tokens.size() - 1));
      context.write(BIGRAM, VAL);
    }
  }

    private static final class MyReducer extends Reducer<PairOfStrings, Text, PairOfStrings, HMapStIW> {
    
    @Override
    public void reduce(PairOfStrings key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HMapStIW map = new HMapStIW();
      Iterator<Text> iter = values.iterator();

      while (iter.hasNext()) {
        String k = iter.next().toString();
        if (map.containsKey(k)) {
          map.put(k, map.get(k) + 1);
        } else {
          map.put(k, 1);
        }
      }

      context.write(key, map);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private Dictogram() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-text")
    boolean text = false;
  }

  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
    parser.parseArgument(argv);

    LOG.info("Tool name: " + Dictogram.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(Dictogram.class.getSimpleName());
    job.setJarByClass(Dictogram.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(HMapStIW.class);
    if (args.text) {
      job.setOutputFormatClass(TextOutputFormat.class);
    } else {
      job.setOutputFormatClass(MapFileOutputFormat.class);
    }

    job.setMapperClass(MyMapper.class);
    // job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Dictogram(), args);
  }
}
