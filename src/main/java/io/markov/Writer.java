package io.markov;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.map.HMapStIW;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

public class Writer extends Configured implements Tool {
  private ArrayList<MapFile.Reader> indices;

  private Writer() {}

  private void initialize(String indexPath, FileSystem fs) throws IOException {
    indices = new ArrayList<>();
    FileStatus[] fileStatuses = fs.listStatus(new Path(indexPath));
    
    for (FileStatus status : fileStatuses) {
      String fileName = status.getPath().getName();
      if (fileName.startsWith("part-r-")) {
        indices.add(new MapFile.Reader(new Path(indexPath + "/" + fileName), fs.getConf()));
      }
    }
  }

  private String write(PairOfStrings bigram, int words) throws IOException {
    HMapStIW set = new HMapStIW();
    String line = new String();
    String[] nextText;
    
      while (words > 0) {
        for (MapFile.Reader ind: indices) {
          if (ind.get(bigram, set) != null) {
          
            Object[] keySet = set.keySet().toArray();
            Random r = new Random();
            int random = r.nextInt(keySet.length);
            nextText = ((String) keySet[random]).split(" ");
            String next = nextText[0];
            
            if (nextText.length > 1) {
              line = line.concat(next);
              next = nextText[1];
              bigram.set(nextText[0], next);
            } else {
              bigram.set(bigram.getRightElement(), next);
            }

            line = line.concat(" ");              
            line = line.concat(next);
            break;
          }
        }
        --words;
      }
    return line;

  }

  private void createLines() throws IOException {
    
    int lines = 3;
    
    while (lines > 0) {
      PairOfStrings bigram = new PairOfStrings("*START*", "*START*");
      String line = new String();
      line = line.concat(write(bigram, 8));
      System.out.println(line);
      --lines;
    }
  }


  private static final class Args {
    
    @Option(name = "-lineSize", metaVar = "[path]", required = false)
    int lineSize = 15;
    
    @Option(name = "-paragraphs", metaVar = "[path]", required = true, usage = "index path")
    int paragraphs = 1;
  }


  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    FileSystem fs = FileSystem.get(new Configuration());
    initialize("output", fs);
    
    long startTime = System.currentTimeMillis();
    createLines();
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Writer(), args);
  }
}