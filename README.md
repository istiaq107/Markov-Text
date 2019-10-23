# Markov Text Generator

2nd-order markov model generated using Hadoop MapReduce to generate texts.
Starter code was taken from [Bespin](https://github.com/lintool/bespin) (Bigram Count in particular),
and modified accordingly to create state-machine that mimics Markov Model.

# Upcoming updates

The model is going to be trained over a corpus of 16B tokens to ensure more accurate randomizations of generated texts. Following that, a new update would include weighted state-machines to leverage end-of-line tags to make the line endings 
more logical. This will be done using weights that are calculated based on how far the tokens were from the end of line
in the original corpus, and decisions would be made based on that to end of lines in generated texts.
