package io.markov;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Tokenizer so that we have a consistent definition of a "word".
 */
public class Tokenizer {
  private static final Pattern PATTERN = Pattern.compile("(^[^a-z]+|[^a-z]+$)");

  public static List<String> tokenize(String input) {
    List<String> tokens = new ArrayList<>();
    StringTokenizer itr = new StringTokenizer(input);
    while (itr.hasMoreTokens()) {
      String w = PATTERN.matcher(itr.nextToken().toLowerCase()).replaceAll("");
      if (w.length() != 0) {
        tokens.add(w);
      }
    }

    return tokens;
  }
}
