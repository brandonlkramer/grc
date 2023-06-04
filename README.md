# Graded Reader Analyses Files
## Created by Brandon Kramer, for his PhD Thesis titled "The Vocabulary of Graded Readers"

These are the code files for an analysis of the vocabulary in graded readers, both single-words and multi-word expressions.

### Corpus Cleaning (Segmenter.py)
The segmenter.py file runs a script which cleans the files for errors where tokens are not separated properly using the module "segmenter". 
  First, each text file is opened and small issues are cleaned through a REGEX find-and-replace search. These issues included double spaces, lack of a space after punctuation, and stylized fonts being used for the quotation marks. Cleaning these issues first helps the script to parse the text more consistently. 
  Next, each token is isolated along with the context sentence. In this case a token was defined by non-digit characters surrounded by spaces or punctuation. Each token is then quickly checked for its presence in Nation’s BNC/COCA word lists (including the standard lists, proper noun lists, and interjection lists).
  If a word is not present in the above lists, it is run through an open-source script called segmenter which is designed to separate words which are connected together. For example, when run on the example text "togetherwith" it would return the most likely segmentation, "together with" (Jenks, 2018). If no likely segmentations are found, this script simply returns the target token. 
  The user of the script is then presented with every off-list token and its context sentence, along with three options: (1) Replace the target token with the suggested segmentation, (2) Reject the suggestion and keep the target token if it was judged to be correct (usually proper nouns and foreign language words), and (3) Manually input a replacement for the target token. 
  Each decision is recorded in an external file, which is referenced in every iteration then on for automatic replacements. 

### Single Word Analyses and Database Setup (setup.py)
The setup.py file creates and populates the sqlite database used for the single word analyses. It also contains the scripts for the descriptive statistics and creates coverage tables which were used to answer Research Questions 1 through 5. 

### Coverage Data (1_single_word_data_spacy.csv)
This is the output data file for all 1872 graded readers in the Graded Reader corpus. 

### Appendix A Table (Appendix_A.txt)
Basic descriptive info for all books in the Graded Reader Corpus.

### Appendix B Table (Appendix_B.txt)
Table showing the composition of the Graded Reader Corpus by Publisher and Series.

### Appendix D Table (Appendix_D.txt)
Descriptive table showing the coverage figures for all graded readers in the GRC, organized by publisher and series

### Coverage Figure Scripts (Looping APA Tables.R) 
This script creates coverage tables for all books in the previously described output file based on 90%, 95%, and 98% lexical coverage requirements based on the following word lists: New GSL (Brezina & Gablasova, 2014); NGSL (Browne, et al., 2013); BNC/COCA (Nation, 2020); and the SEWK-J (Mizumoto et al., 2020; Pinchbeck, in preparation). These graphs were used in Chapter 4, "Results of Single Word Analyses".

### Data Extraction for Research Question 6 (RQ6_final.ipynb)
This file extracts all 2-, 3-, 4-, and 5-grams from the corpora which occur over 20 times per million tokens. It does this for the COCA as a whole, the COCA fiction corpus, the GRC, and just the fiction texts within the GRC.

### Data Extraction for Research Questions 7 and 8 (RQ7-8.ipynb)
This extracts the counts for all PHRASE list items (Martinez & Schmitt, 2012) and PHaVE list items (Garnier & Schmitt, 2015) within the GRC. 

