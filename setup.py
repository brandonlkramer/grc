import sqlite3, os, os.path
import glob, requests, re
import spacy
import textstat, textwrap
import time

start_time = time.time()

try:
    os.remove('0a_graded_readers.sqlite') # This prevents errors by deleting the previous database file before starting
except:
    print("no previous database file found")
try:
    os.remove('/Users/brandon/Documents/Database_Files/0a_graded_readers.sqlite') # This prevents errors by deleting the previous database file before starting
except:
    print("no previous database file found")

conn = sqlite3.connect('/Users/brandon/Documents/Database_Files/0a_graded_readers.sqlite') # Creates and connects to database where i will keep data
#conn = sqlite3.connect('0a_graded_readers.sqlite')  
c = conn.cursor() 

c.execute("""CREATE TABLE corpus_spacy (
    file_id INTEGER,
    file_location INTEGER,
    type TEXT,
    type_lc TEXT,
    pos TEXT,
    tag TEXT,
    pos_tree_conv TEXT,
    lemma_ TEXT,
    lemma_pos TEXT,
    lexicon_id INTEGER,
    dependency_text TEXT,
    dependency_tag TEXT,
    dependency_pos TEXT,
    dependency_lemma TEXT)""")

# Creates a table for the corpus_info so it will play nicely with available tools
c.execute("""CREATE TABLE corpus_info (
    id INTEGER PRIMARY KEY,
    name TEXT,
    file_count INTEGER,
    token_count INTEGER,
    encoding TEXT)""")

# Creates a table for the document files in the corpus
c.execute("""CREATE TABLE files (
    id INTEGER PRIMARY KEY,
    book_info_id INTEGER,
    file_name TEXT,
    source TEXT,
    character_count INTEGER,
    sentence_count INTEGER,
    token_count INTEGER,
    xreading INTEGER,
    library INTEGER,
    text TEXT,
    flesch_kincaid_grade REAL,
    flesch_reading_ease REAL)""")


# Creates a table for the book meta-data
c.execute("""CREATE TABLE book_info (
    id INTEGER PRIMARY KEY,
    series_id INTEGER,
    ERF_code TEXT,
    title TEXT,
    authors TEXT,
    publisher_level TEXT,
    publisher_headwords INTEGER,
    erf_level_id INTEGER,
    cefr_text TEXT,
    cefr_level_id INTEGER,
    yomiyasusa_text TEXT,
    yomiyasusa_ave REAL,
    word_count_reported INTEGER,
    add_material_word_count INTEGER,
    actual_or_estimate TEXT,
    page_count INTEGER,
    mreader_quiz_available TEXT,
    mreader_scale INTEGER,
    fiction_nonfiction TEXT,
    genre TEXT,
    target_group TEXT,
    isbn_a TEXT,
    isbn_b TEXT,
    isbn_c TEXT,
    isbn_d TEXT,
    isbn_e TEXT,
    audio_available TEXT,
    copyright_year INTEGER,
    year_published_first INTEGER,
    in_print TEXT,
    notes TEXT)""")

# Creates a table for all word lemmas, flemmas, and word families in the corpus 
c.execute("""CREATE TABLE lexicon (
    id INTEGER PRIMARY KEY,
    type TEXT,
    lemma_pos_tree TEXT,
    lemma TEXT,
    pos_spacy TEXT,
    pos_tree TEXT,
    new_gsl_rank INTEGER,
    ngsl_headword TEXT,
    ngsl_rank INTEGER,
    sewk_j_headword TEXT,
    sewk_j_rank INTEGER,
    family_headword TEXT,
    bnc_coca_rank INTEGER,
    frequency_spacy INTEGER,
    range_spacy INTEGER)""")

#Creates tables for the ERF and CEFR levels
c.execute("""CREATE TABLE erf_levels (
    id INTEGER PRIMARY KEY,
    erf_sublevel_text TEXT,
    erf_sublevel_expl TEXT,
    erf_level_num INTEGER,
    erf_level_text TEXT,
    erf_level_expl TEXT)""")

c.execute("""CREATE TABLE cefr_levels (
    id INTEGER PRIMARY KEY,
    cefr_level_text TEXT)""")

#Creates tables to store book publishers and series
c.execute("""CREATE TABLE series (
    id INTEGER PRIMARY KEY,
    series_name TEXT,
    publisher_id INTEGER)""")

c.execute("""CREATE TABLE publishers (
    id INTEGER PRIMARY KEY,
    publisher TEXT)""")

print("Database created successfully") # Not necessary but helps feel like program is running

conn.commit() # Pushes everything to database

######################################################################################
# Populate book_info table (ERF Graded Reader Database) and other basic tables
######################################################################################
print("Uploading book_info table ...")

# Removes invalid characters replaces them with utf-8
def remove_invalid_chars(byte_sequence): 
    # Replace invalid characters with the replacement character (�)
    byte_sequence = byte_sequence.replace(b'\xca', b'\ufffd')
    return byte_sequence.decode("utf-8")

# Opens the "book_info" file and loads it to the database
try: 
    book_info = open('@_database_setup_uploads/book_info_updated.txt', 'rb').read()
    book_info = remove_invalid_chars(book_info)
except UnicodeDecodeError:
    book_info = open('@_database_setup_uploads/book_info.txt', 'r', encoding = 'iso-8859-1').read()
    print("using iso-8859-1 format...")
book_info_split = book_info.split('\n')
count = 0
for line in book_info_split:
    count += 1
    line = line.split('\t')
    c.execute("INSERT INTO book_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(line))  

# Uploads the "files" data with information about the book files
files = open('@_database_setup_uploads/files.txt', 'r', encoding = 'utf-8').read()
files = files.split('\n')
for line in files:
    line = line.split('\t')
    c.execute("INSERT INTO files (id, book_info_id, file_name, source, xreading, library) VALUES (?,?,?,?,?,?)",(line)) 

conn.commit()

# remove the quotation marks from the file_name column in the files table
c.execute("UPDATE files SET file_name = replace(file_name, '\"', '')")
c.execute("UPDATE book_info SET title = replace(title, '\"', '')")
c.execute("UPDATE book_info SET authors = replace(authors, '\"', '')")
c.execute("UPDATE book_info SET publisher_headwords = replace(publisher_headwords, '\"', '')")
c.execute("UPDATE book_info SET word_count_reported = replace(word_count_reported, '\"', '')")
conn.commit()

# Uploads the "publishers" list
files = open('@_database_setup_uploads/publishers.txt', 'r', encoding = 'utf-8').read()
files = files.split('\n')
for line in files:
    line = line.split('\t')
    c.execute("INSERT INTO publishers (id, publisher) VALUES (?,?)",(line)) 

# Uploads the "series" list
files = open('@_database_setup_uploads/series.txt', 'r', encoding = 'utf-8').read()
files = files.split('\n')
for line in files:
    line = line.split('\t')
    c.execute("INSERT INTO series VALUES (?,?,?)",(line)) 

# Uploads the "erf_levels" list
files = open('@_database_setup_uploads/erf_levels.txt', 'r', encoding = 'utf-8').read()
files = files.split('\n')
for line in files:
    line = line.split('\t')
    c.execute("INSERT INTO erf_levels VALUES (?,?,?,?,?,?)",(line)) 

# Uploads the "cefr_levels" list
files = open('@_database_setup_uploads/cefr_levels.txt', 'r', encoding = 'utf-8').read()
files = files.split('\n')
for line in files:
    line = line.split('\t')
    c.execute("INSERT INTO cefr_levels (id, cefr_level_text) VALUES (?,?)",(line)) 

conn.commit() # Pushes everything to database

######################################################################################
### Populate word frequency lists
######################################################################################
print("Uploading word frequency lists...")

c.execute("CREATE INDEX lexicon_index ON lexicon (type)")
conn.commit()

# Uploads the lexicon table
new_gsl_path = '@_database_setup_uploads/Word_Lists/new_gsl.txt'
ngsl_path = '@_database_setup_uploads/Word_Lists/ngsl'
ngsl_ranks_path = '@_database_setup_uploads/Word_Lists/ngsl_ranks.txt'
sewk_j_path = '@_database_setup_uploads/Word_Lists/sewk_j.txt'
bnc_coca_path = '@_database_setup_uploads/Word_Lists/bnc_coca'  

with open('@_database_setup_uploads/hashtable.txt', 'w') as output_file:
    output_file.write('')

with open(new_gsl_path, 'r') as file:
    text = file.read()
    text = text.upper()
    text = text.split('\n')
    id = 1
    for line in text:
        line = line.split('\t')
        lemma_ = line[0]
        pos = line[1]
        lemma_pos = line[0] + '_' + line[1]
        rank = line[2]
        insertion = (id, lemma_, lemma_pos, lemma_, pos, rank)
        c.execute("""INSERT INTO lexicon (
                    id, 
                    type, 
                    lemma_pos_tree, 
                    lemma, 
                    pos_tree, 
                    new_gsl_rank) 
                VALUES (?, ?, ?, ?, ?, ?)""", insertion)
        id += 1
    conn.commit()

with open(sewk_j_path, 'r') as file:
    text = file.read()
    text = text.upper()
    text = text.split('\n')
    for line in text:
        line = line.split('\t')
        word = line[0]
        headword = line[1]
        rank = line[2]
        c.execute("""SELECT id 
                    FROM lexicon 
                    WHERE type = ?""", (word,))
        matching_id = c.fetchone()
        if matching_id is None:
            insertion = (id, word, headword, rank)
            c.execute("""INSERT INTO lexicon (
                        id, 
                        type, 
                        sewk_j_headword, 
                        sewk_j_rank) 
                    VALUES (?, ?, ?, ?)""", insertion)
            id += 1
            conn.commit()
        else:
            c.execute("""UPDATE lexicon 
                        SET sewk_j_headword = ?, 
                        sewk_j_rank = ? 
                        WHERE type = ?""", (headword, rank, word))
            conn.commit()

for filename in os.listdir(ngsl_path):
    if filename == ".DS_Store":
        continue
    file_path = os.path.join(ngsl_path, filename)
    if os.path.isdir(file_path):
        continue
    with open(file_path, 'r') as file:
        text = file.read()
        text = text.upper()
        text = re.sub('\n\t', ' ', text)
        text = re.sub(r'^(\w+)([ \n])', r'\1 \1\2', text, flags=re.MULTILINE)
        while True:
            new_text = re.sub(r'^(\w+) (\w+) ([\w ]+)\n', r'\1 \2\n\1 \3\n', text, flags=re.MULTILINE)
            if new_text == text:
                break
            text = new_text
    filename = filename.replace('_basewrd.txt','')  
    filename = filename.replace('_NGSL','9999')
    filename = filename.replace('1st','9999')
    filename = filename.replace('2nd','9999')
    filename = filename.replace('3rd','9999')
    filename = filename.replace('Suplemental','0')
    add_level = ' ' + filename + '\n'
    text = text.replace('\n',add_level)
    text = text.split('\n')
    for line in text:
        line = line.split(' ')
        if len(line) == 1:
            continue
        headword = line[0]
        word = line[1]
        rank = int(line[2])
        c.execute("""SELECT id
                    FROM lexicon
                    WHERE type = ?""", (word,))
        matching_id = c.fetchone()
        if matching_id is None:
            insertion = (id, word, headword, rank)
            c.execute("""INSERT INTO lexicon (
                        id, 
                        type, 
                        ngsl_headword, 
                        ngsl_rank) 
                    VALUES (?, ?, ?, ?)""", insertion)
            id += 1
            conn.commit()
        else:
            c.execute("""UPDATE lexicon 
                        SET ngsl_headword = ?, 
                        ngsl_rank = ? 
                        WHERE type = ?""", (headword, rank, word))
            conn.commit()

with open(ngsl_ranks_path, 'r') as file:
    text = file.read()
    text = text.upper()
    text = text.split('\n')
    rank = 1
    for line in text:
        line = line.upper()
        c.execute("UPDATE lexicon SET ngsl_rank = ? WHERE ngsl_headword = ?", (rank, line))
        conn.commit()
        rank += 1

for filename in os.listdir(bnc_coca_path):
    if filename == ".DS_Store":
        continue
    file_path = os.path.join(bnc_coca_path, filename)
    if os.path.isdir(file_path):
        continue
    with open(file_path, 'r') as file:
        text = file.read()
        text = text.upper()
        text = re.sub('\n\t', ' ', text)
        text = re.sub(r'^(\w+)([ \n])', r'\1 \1\2', text, flags=re.MULTILINE)
        while True:
            new_text = re.sub(r'^(\w+) (\w+) ([\w ]+)\n', r'\1 \2\n\1 \3\n', text, flags=re.MULTILINE)
            if new_text == text:
                break
            text = new_text
    filename = filename.replace('basewrd','')
    filename = filename.replace('.txt','')
    add_level = ' ' + filename + '\n'
    text = text.replace('\n',add_level)
    text = text.split('\n')
    for line in text:
        line = line.split(' ')
        if len(line) != 3:
            continue
        headword = line[0]
        word = line[1]
        rank = int(line[2])
        c.execute("""SELECT id
                    FROM lexicon
                    WHERE type = ?""", (word,))
        matching_id = c.fetchone()
        if matching_id is None:
            insertion = (id, word, headword, rank)
            c.execute("""INSERT INTO lexicon (
                        id, 
                        type, 
                        family_headword, 
                        bnc_coca_rank) 
                    VALUES (?, ?, ?, ?)""", insertion)
            id += 1
            conn.commit()
        else:
            c.execute("""UPDATE lexicon 
                        SET family_headword = ?, 
                        bnc_coca_rank = ? 
                        WHERE type = ?""", (headword, rank, word))
            conn.commit()


new_gsl_search_terms = (("KIND_OF","KIND"), ("A_LOT","LOT"), ("LOTS_OF","LOTS"), ("ACCORDING_TO","ACCORDING"), \
                        ("SORT_OF","SORT"), ("SUBJECT_TO","SUBJECT"), ("THANKS_FOR","THANKS"), \
                        ("THANKS_TO","THANKS"), ("PRIOR_TO","PRIOR"), ("REGARDLESS_OF","REGARDLESS"), \
                        ("PER_CENT","PERCENT"), ("LONG-TERM","LONGTERM"))
for search, replace in new_gsl_search_terms:
    c.execute("""UPDATE lexicon SET 
                ngsl_headword = (SELECT ngsl_headword FROM lexicon WHERE type = ?),
                ngsl_rank = (SELECT ngsl_rank FROM lexicon WHERE type = ?),
                sewk_j_headword = (SELECT sewk_j_headword FROM lexicon WHERE type = ?),
                sewk_j_rank = (SELECT sewk_j_rank FROM lexicon WHERE type = ?),
                family_headword = (SELECT family_headword FROM lexicon WHERE type = ?),
                bnc_coca_rank = (SELECT bnc_coca_rank FROM lexicon WHERE type = ?)              
                WHERE type = ?""", (replace, replace, replace, replace, replace, replace, search))
    conn.commit()


######################################################################################
# This is where the corpus itself is uploaded to the database
######################################################################################
print("Uploading corpus to database...")

# This will find the file_id using the file_name
def file_id_lookup(file_name):
    # Adds the record - NEEDS TO BE ADJUSTED - (can remove the "id," if i don't want that)
    c.execute("SELECT id FROM files WHERE file_name = (?)", (file_name,))
    id = c.fetchall()
    try:
        id = id[0][0]
    except:
        id = "FILE NOT FOUND"
    return id

# The script below converts all British English to American English spelling
# found here: https://stackoverflow.com/questions/42329766/python-nlp-british-english-vs-american-english
def americanize(string):
    url ="https://raw.githubusercontent.com/hyperreality/American-British-English-Translator/master/data/british_spellings.json"
    british_to_american_dict = requests.get(url).json()    
    for british_spelling, american_spelling in british_to_american_dict.items():
        string = string.replace(british_spelling, american_spelling)
    return string

#create a list that includes all files in the directory folder that end in ".txt"
#filenames = glob.glob("TestFiles" + "/*.txt")
#This is the proper list of books
filenames = glob.glob("XreadingBooks_20Jan2022" + "/*.txt") + glob.glob("scanned_books" + "/*.txt")

nlp = spacy.load("en_core_web_sm") #This loads the SpaCy model
textstat.set_lang("en_US")
xml_regex = re.compile(r'<.*?>')
count = 1

c.execute("CREATE INDEX filename_index ON files (file_name)")
conn.commit()

for filename in filenames: #Loops through each text file one at a time
    print("Processing file " + str(count) + " of " + str(len(filenames)) + "...")
    count += 1
    text = open(filename, errors = "ignore").read() #opening the file as a string
    text = re.sub(xml_regex, '', text)
    text = americanize(text)
    # regex search to remove * from after words, often used as a gloss marker
    text = text.replace("*", "")
    # Regex search for text followed by numerals like "time73" and replace with "time" (some books have glossed words like this)
    text = re.sub(r'(\w+)(\d+)', r'\1', text)
    text = re.sub(r'(\w+)(\d+)', r'\1', text)
    text = re.sub(r"’", r"'", text) # cleans non-uft-8 apostrophes
    text = re.sub(r'[“”]', r'"', text) # cleans non-uft-8 quotation marks
    spacyText = nlp(text) #tags and processes the text
    filename = filename.replace("TestFiles/","")
    filename = filename.replace("XreadingBooks_20Jan2022/","")
    filename = filename.replace("scanned_books/","")
    file_id = file_id_lookup(filename)
    sentences = len(list(spacyText.sents)) #this counts the number of sentences in the text
    characters = len(text.replace("\n","")) #this counts the number of characters in the text without line feeds
    try:
        flesch_kincaid_grade = textstat.flesch_kincaid_grade(text)
        flesch_reading_ease = textstat.flesch_reading_ease(text)
    except:
        flesch_kincaid_grade = "NA"
        flesch_reading_ease = "NA"

    # Update the files table with the number of sentences and characters
    c.execute("UPDATE files SET sentence_count = (?) WHERE id = (?)", (sentences,file_id))
    c.execute("UPDATE files SET character_count = (?) WHERE id = (?)", (characters,file_id))
    c.execute("UPDATE files SET text = (?) WHERE id = (?)", (text,file_id))
    c.execute("UPDATE files SET flesch_kincaid_grade = (?) WHERE id = (?)", (flesch_kincaid_grade,file_id))
    c.execute("UPDATE files SET flesch_reading_ease = (?) WHERE id = (?)", (flesch_reading_ease,file_id))
    c.execute("UPDATE files SET corpus_id = (?) WHERE id = (?)", ("1",file_id))

    file_location = 0 #this variable counts the location of words within each document
    for unit in spacyText:
        if unit.pos_ == "SPACE": #this tells it to skip items that are spaces
            continue
        else: 
            file_location += 1
            # Connect to database and create cursor
            c.execute("INSERT INTO corpus_spacy (file_id,file_location,type,type_lc,pos,tag,lemma_,dependency_text,\
                      dependency_tag, dependency_pos, dependency_lemma) VALUES (?,?,?,?,?,?,?,?,?,?,?)", \
                        (file_id,file_location,unit.text,unit.text.lower(),unit.pos_.upper(),unit.tag_.upper(),\
                         unit.lemma_, unit.head.text.upper(), unit.head.tag_, unit.head.pos_, unit.head.lemma_.upper()))
            continue

# Commit our command and close our connection
c.execute("DROP INDEX filename_index")
conn.commit()

c.execute("CREATE INDEX corpus_pos_idx ON corpus_spacy (pos)")
c.execute("CREATE INDEX corpus_tag_idx ON corpus_spacy (tag)")
c.execute("CREATE INDEX corpus_pos_tag_idx ON corpus_spacy (pos, tag)")
conn.commit()

######################################################################################
# Clean the corpus tables
######################################################################################
print("Cleaning the corpus table...")

# Spacy has a tagging quirk where it lists the lemma form of 's as simply an apostrophe. This fixes that.
search_term = "'s"
query = "SELECT rowid, lemma_ FROM corpus_spacy WHERE type_lc = ? AND pos != 'PUNCT' AND pos != 'AUX'"
for result in c.execute(query, (search_term,)).fetchall():
    c.execute("UPDATE corpus_spacy SET lemma_ = 's' WHERE rowid = ?", (result[0],))

# Removes periods from Mr, Mrs, Ms, & Dr tagged as PROPN, which was causing issues
c.execute("UPDATE corpus_spacy SET type = 'Mrs', type_lc = 'mrs', lemma_ = 'Mrs', lemma_pos = 'MRS_PROPN' WHERE type = 'Mrs.' AND pos = 'PROPN'")
c.execute("UPDATE corpus_spacy SET type = 'Mr', type_lc = 'mr', lemma_ = 'Mr', lemma_pos = 'MR_PROPN' WHERE type = 'Mr.' AND pos = 'PROPN'")
c.execute("UPDATE corpus_spacy SET type = 'Dr', type_lc = 'dr', lemma_ = 'Dr', lemma_pos = 'DR_PROPN' WHERE type = 'Dr.' AND pos = 'PROPN'")
c.execute("UPDATE corpus_spacy SET type = 'Ms', type_lc = 'ms', lemma_ = 'Ms', lemma_pos = 'MS_PROPN' WHERE type = 'Ms.' AND pos = 'PROPN'")
c.execute("UPDATE corpus_spacy SET lemma_ = 'clothes', lemma_pos = 'CLOTHES_N' WHERE lemma_ = 'clothe' AND pos = 'NOUN'")

# Find dashes and other symbols that are not punctuation and change pos to "PUNCT" (Ex: in the middle of names, "Jeun-Lee", were set as NOUN)
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '-' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '>' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '<' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '?' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '!' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '.' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '''' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '&' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '%' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '$' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '#' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '@' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '¥' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '€' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '£' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '_' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '*' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '(' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = ')' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '[' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = ']' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '+' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '=' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '{' AND pos != 'PUNCT'")
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = '}' AND pos != 'PUNCT'")

conn.commit()

# This marks apostrophes and 's as punctuation, so that they are removed from analyses and not counted as unique words (actually possessive 's)
replacements = (("'s","PART"),("'","PART"))
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE lemma_ = ? AND pos = ?",replacements[0])
c.execute("UPDATE corpus_spacy SET pos = 'PUNCT' WHERE type_lc = ? AND pos = ?",replacements[1])
conn.commit()

# mark all cardinal numbers as tag CD when they are numerals as defined by regex (Ex: 3; 4.2; 5,000; 3rd)
c.execute("UPDATE corpus_spacy SET pos = 'CD' WHERE (tag = 'CD' OR tag = 'JJ' OR tag = 'NN' OR tag = 'NNP') AND (type_lc LIKE '1%'\
    OR type_lc LIKE '2%' OR type_lc LIKE '3%' OR type_lc LIKE '4%' OR type_lc LIKE '5%'\
    OR type_lc LIKE '6%' OR type_lc LIKE '7%' OR type_lc LIKE '8%' OR type_lc LIKE '9%')")
conn.commit()

# Replacing various forms in lemma_ column to match word lists (Ex: "café" to "cafe"; "n't" to "t"; "o'clock" to "oclock")
apostrophe_replacements = (("'d","d"),("'ll","ll"),("'m","be"),("'re","be"),("'s","s"),("'ve","ve"),("n't","t"),("o'clock","oclock"),("café","cafe"))
for before,after in apostrophe_replacements:
    c.execute("UPDATE corpus_spacy SET lemma_ = ? WHERE lemma_ = ?", (after,before))
    c.execute("UPDATE corpus_spacy SET type_lc = ? WHERE type_lc = ?", (after,before))


# if punct or sym simply paste it into the other column
spacy_tag_conversion = (("AFX","UNC"),("CC","con"),("CD","x"),("DT","x"),("EX","e"),("FW","UNC"),("HYPH","PUNCT"),\
    ("IN","con"),("JJ","adj"),("JJR","adj"),("JJS","adj"),("LS","PUNCT"),("MD","mod"),("NIL","UNC"),("NN","n"),\
    ("NNP","PROPN"),("NNPS","n"),("NNS","n"),("PDT","x"),("POS","POS"),("PRP","pron"),("PRP$","pron"),("RB","adv"),\
    ("RBR","adv"),("RBS","adv"),("RP","adv"),("TO","t"),("UH","INTER"),("VB","v"),("VBD","v"),("VBG","v"),\
    ("VBN","v"),("VBP","v"),("VBZ","v"),("WDT","x"),("WP","pron"),("WP$","pron"),("WRB","adv"),\
    ("SP","SPACE"),("ADD","PUNCT"),("NFP","PUNCT"),("GW","UNC"),("XX","UNC"),("BES","v"),("HVS","v"),("_SP","SPACE"))
for before,after in spacy_tag_conversion:
    c.execute("UPDATE corpus_spacy SET pos_tree_conv = ? WHERE tag = ?", (after.upper(),before))

c.execute("UPDATE corpus_spacy SET pos_tree_conv = 'PUNCT' WHERE pos = 'PUNCT'")

# update the lemma_pos column in the corpus_spacy table to be a composite of the lemma and pos columns
c.execute("UPDATE corpus_spacy SET lemma_pos = upper(lemma_) || '_' || upper(pos_tree_conv)")

# issue with "can't" not matching to the database because of how it is parsed by spacy
c.execute("UPDATE corpus_spacy SET type_lc = 'can' WHERE type_lc = 'ca' AND lemma_ = 'can'")
c.execute("UPDATE corpus_spacy SET type_lc = 'have' WHERE type_lc = 've' AND lemma_ = 'have'")
conn.commit()


######################################################################################
# Next we will connect the lexicon table to the corpus tables
######################################################################################
print("Connecting the lexicon table to the corpus tables...")

# Create indices for each list above to speed up corpus linking
print("Creating indices...")

c.execute("CREATE INDEX lemma_pos_tree_index ON lexicon (lemma_pos_tree)")
c.execute("CREATE INDEX lemma_pos_corpus_index ON corpus_spacy (lemma_pos)")
c.execute("CREATE INDEX spacy_index ON corpus_spacy (type_lc)")
c.execute("CREATE INDEX pos_index ON corpus_spacy (pos)")
c.execute("CREATE INDEX tag_index ON corpus_spacy (tag)")

conn.commit() # Pushes everything to database

# updates the corpus tables with the id of the type in the lexicon table based on lemma_pos_tree
print("Connecting the lexicon table to the corpus_spacy table...")
c.execute("UPDATE corpus_spacy SET lexicon_id = (SELECT id FROM lexicon WHERE corpus_spacy.lemma_pos = lexicon.lemma_pos_tree AND corpus_spacy.pos NOT IN ('PUNCT', 'SYM', 'CD') AND corpus_spacy.tag != 'POS')")
conn.commit()


## CORPUS_SPACY - UPDATE LEXICON ##################

# first update lexicon_id in the corpus_spacy table
c.execute("UPDATE corpus_spacy SET lexicon_id = (SELECT id FROM lexicon WHERE corpus_spacy.lemma_pos = lexicon.lemma_pos_tree AND corpus_spacy.pos NOT IN ('PUNCT', 'SYM', 'CD') AND corpus_spacy.tag != 'POS')")
conn.commit()

# Collect all row information for rows in the corpus_spacy table where lexicon_id is null, and pos is not PUNCT or SYM, and upper(corpus_spacy.type_lc) = lexicon.type
c.execute("SELECT DISTINCT type_lc, pos, pos_tree_conv, lemma_, lemma_pos FROM corpus_spacy WHERE lexicon_id IS NULL AND pos NOT IN ('PUNCT', 'SYM', 'CD') AND tag != 'POS'")
matching_rows = c.fetchall()

for row in matching_rows:
    c.execute("UPDATE lexicon SET lemma_pos_tree = ?, lemma = ?, pos_tree = ?, pos_spacy = ? WHERE type = ? AND (lemma = '' OR lemma IS NULL)", 
    (row[4],row[3],row[2],row[1],row[0].upper()))

# update lexicon_id in the corpus_spacy table
c.execute("UPDATE corpus_spacy SET lexicon_id = (SELECT id FROM lexicon WHERE corpus_spacy.lemma_pos = lexicon.lemma_pos_tree AND corpus_spacy.pos NOT IN ('PUNCT', 'SYM', 'CD') AND corpus_spacy.tag != 'POS')")
conn.commit()

# Collect remaining new lemmas and add them to the lexicon table with the appropriate flemma and family information
# Collect distinct row information for rows in the corpus_spacy table where lexicon_id is null, and pos is not PUNCT or SYM, 
# and upper(corpus_spacy.type_lc) = lexicon.type
c.execute("SELECT DISTINCT type_lc, pos, pos_tree_conv, lemma_, lemma_pos FROM corpus_spacy WHERE lexicon_id IS NULL AND pos NOT IN ('PUNCT', 'SYM', 'CD') AND tag != 'POS'")
matching_rows = c.fetchall()

# find the maximum id in the lexicon table
c.execute("SELECT MAX(id) FROM lexicon")
new_id_counter = c.fetchone()[0]

for row in matching_rows:
    new_id_counter += 1
    # Collect the ngsl_headword, ngsl_rank, sewk_j_headword, sewk_j_rank, family_headword, and bnc_coca_rank 
    # for the row in the lexicon table where type = upper(corpus_spacy.type_lc) and pos.tree != ''
    c.execute("SELECT ngsl_headword, ngsl_rank, sewk_j_headword, sewk_j_rank, family_headword, bnc_coca_rank \
    FROM lexicon WHERE type = ? AND pos_tree != '' AND pos_tree IS NOT NULL", (row[0].upper(),))
    lexicon_info = c.fetchall()
    if len(lexicon_info) == 0:
        #then it needs to be added to the lexicon table using only the information available in the corpus_tree table
        c.execute("INSERT INTO lexicon (id, type, lemma_pos_tree, lemma, pos_spacy, pos_tree, ngsl_headword, sewk_j_headword, family_headword) \
            VALUES (?,?,?,?,?,?,?,?,?)", (new_id_counter,row[0].upper(),row[4],row[3],row[1],row[2],row[0].upper(),row[0].upper(),row[0].upper()))
        continue
    else:
        lexicon_info = lexicon_info[0]
        # Create a new row in the lexicon table where type = upper(corpus_spacy.type_lc) and the rest of the information in the row is also populated
        c.execute("INSERT INTO lexicon (id,type, lemma_pos_tree, lemma, pos_spacy, pos_tree, ngsl_headword, ngsl_rank, sewk_j_headword, \
            sewk_j_rank, family_headword, bnc_coca_rank) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", \
            (new_id_counter,row[0].upper(),row[4],row[3],row[1],row[2],lexicon_info[0],lexicon_info[1],lexicon_info[2],lexicon_info[3],lexicon_info[4],lexicon_info[5]))

conn.commit()



### Extra cleaning to allow for multiple word sets in the new gsl
# update lexicon_id in the corpus_spacy table
c.execute("UPDATE corpus_spacy SET lexicon_id = (SELECT id FROM lexicon WHERE corpus_spacy.lemma_pos = lexicon.lemma_pos_tree AND corpus_spacy.pos NOT IN ('PUNCT', 'SYM', 'CD') AND corpus_spacy.tag != 'POS')")

conn.commit()

c.execute("CREATE INDEX lemma_index ON corpus_spacy (lemma_)")
conn.commit()
new_gsl_search_terms = (("kind","of","0"), ("a","lot","1"), ("lots","of","0"), ("according","to","0"), \
                        ("sort","of","0"), ("subject","to","0"), ("thanks","for","0"), \
                        ("thanks","to","0"), ("prior","to","0"), ("regardless","of","0"))
for term in new_gsl_search_terms:
    query = """SELECT c1.rowid 
    FROM corpus_spacy AS c1
    JOIN corpus_spacy AS c2
    ON c1.rowid = c2.rowid - 1
    WHERE c1.lemma_ = ? AND c2.lemma_ = ?
    AND c1.file_id = c2.file_id"""
    c.execute(query, (term[0], term[1]))
    rows = c.fetchall()
    for row in rows:
        row = row[0] + int(term[2])
        combined = term[0]+"_"+term[1]
        combined = combined.upper()
        c.execute("UPDATE corpus_spacy SET lexicon_id = (SELECT id FROM lexicon WHERE type = ?) WHERE rowid = ?", (combined, row))
    conn.commit()

query = """SELECT c1.rowid 
    FROM corpus_spacy AS c1
    JOIN corpus_spacy AS c2
    ON c1.rowid = c2.rowid - 1
    WHERE c1.lemma_ = ? AND c2.lemma_ = ?
    AND c1.file_id = c2.file_id"""
c.execute(query, ("per", "cent"))
rows = c.fetchall()
for row in rows:
    row = row[0]
    next_row = int(row) + 1
    combined = "per_cent"
    combined = combined.upper()
    c.execute("""UPDATE corpus_spacy 
    SET lexicon_id = (SELECT id FROM lexicon WHERE type = ?) 
    WHERE rowid = ? OR rowid = ?""", (combined, row, next_row))
conn.commit()


#### Matching biggest differences with spacy to the new gsl tags
matching_issues = (("912","NONE_N"),\
("100","FIRST_ADJ"),\
("107","SUCH_ADJ"),\
("136","MUCH_ADJ"),\
("1367","NOBODY_N"),\
("153","FEW_ADJ"),\
("17","AT_ADV"),\
("175","MOST_ADJ"),\
("180","NEXT_ADV"),\
("1983","EVERYBODY_N"),\
("2184","SOMEBODY_N"),\
("283","TOWARD_CON"),\
("3","OF_ADV"),\
("300","OTHERS_N"),\
("308","SECOND_ADJ"),\
("325","ANYTHING_N"),\
("356","SEVERAL_ADJ"),\
("422","SOMEONE_N"),\
("456","LESS_ADJ"),\
("460","EVERYTHING_N"),\
("558","ANYONE_N"),\
("594","OUTSIDE_ADV"),\
("6","IN_ADV"),\
("614","EVERYONE_N"),\
("647","THIRD_ADJ"),\
("663","ALONE_ADJ"),\
("746","HALF_ADV"),\
("90","AFTER_ADV"),\
("313","ONE_N"),\
("104","MANY_ADJ"),\
("164","SOMETHING_N"),\
("29","WHICH_X"),\
("77","MORE_ADJ"),\
("8","VE_V"),\
("243","NOTHING_N"),\
("5","AN_X"),\
("128","LAST_ADJ"),\
("14","ON_ADV"),\
("886","UP_ADV"),\
("2","S_V"),\
("55","GETS_V"),\
("9","THAT_CON"),\
("369","LOT_N"),\
("50","THERE_E"))

for issue in matching_issues:
    query = "UPDATE lexicon SET new_gsl_rank = ? WHERE lemma_pos_tree = ?"
    c.execute(query, (issue[0], issue[1]))
conn.commit()


######################################################################################
# Next we will populate the corpus_info table
######################################################################################

print("Updating corpus information...")

# get the number of files in the files table
c.execute("SELECT COUNT(id) FROM files")
file_count = c.fetchone()[0]
# get the number of tokens in the corpus table (excluding punctuation)
c.execute("SELECT COUNT(rowid) FROM corpus_spacy WHERE pos NOT IN ('PUNCT', 'SYM', 'CD') AND tag != 'POS'")
token_count = c.fetchone()[0]

line = ("1","English Graded Reader Corpus", file_count, token_count, "utf-8")
c.execute("INSERT INTO corpus_info VALUES (?,?,?,?,?)",line) 

# Commit our command and close our connections
conn.commit()
print("Database setup complete")

######################################################################################
# Update lexicon table with frequency and range information from both corpora
######################################################################################
print("Updating lexicon table with frequency and range information...")

# Delete all indices as we don't need them anymore
c.execute("DROP INDEX IF EXISTS lemma_pos_tree_index")
c.execute("DROP INDEX IF EXISTS lexicon_index")
c.execute("DROP INDEX IF EXISTS tree_index")
c.execute("DROP INDEX IF EXISTS spacy_index")
c.execute("DROP INDEX IF EXISTS lexicon_type_index")
c.execute("DROP INDEX IF EXISTS lemma_index")
conn.commit()

c.execute("CREATE INDEX spacy_lemma_index ON corpus_spacy (lemma_)")
c.execute("CREATE INDEX spacy_lexicon_id_index ON corpus_spacy (lexicon_id)")
c.execute("CREATE INDEX spacy_file_id_index ON corpus_spacy (file_id)")
c.execute("CREATE INDEX lexicon_index ON lexicon (type, pos_tree, lemma)")
c.execute("CREATE INDEX book_info_id_index ON files (book_info_id)")
c.execute("CREATE INDEX erf_index ON book_info (erf_level_id)")
c.execute("CREATE INDEX erf_level_index ON erf_levels (erf_level_num)")
c.execute("CREATE INDEX spacy_type_index ON corpus_spacy (type_lc)")
c.execute("CREATE INDEX fic_nonfic_index ON book_info (fiction_nonfiction)")
conn.commit()

# For each row in the lexicon table, update the frequency columns with the number of times the rowid is referenced in each corpus table
c.execute("UPDATE lexicon SET frequency_spacy = (SELECT COUNT(rowid) FROM corpus_spacy WHERE lexicon_id = lexicon.id)")
# For each row in the lexicon table, update the range columns with the number of unique file_ids referenced in each corpus table
c.execute("UPDATE lexicon SET range_spacy = (SELECT COUNT(DISTINCT file_id) FROM corpus_spacy WHERE lexicon_id = lexicon.id)")

conn.commit()

#############
# CREATE SOME USEFUL VIEWS ###
#############

c.execute('DROP VIEW IF EXISTS meta_data')
conn.commit()

c.execute("CREATE VIEW meta_data AS SELECT book_info.rowid, book_info.series_id, series.series_name, series.publisher_id, publishers.publisher, \
    book_info.title, book_info.fiction_nonfiction, book_info.yomiyasusa_ave, book_info.erf_level_id, erf_levels.erf_sublevel_text, erf_levels.erf_level_num, erf_levels.erf_level_text, \
    book_info.publisher_headwords, files.id AS file_id, files.file_name, files.character_count, files.source, files.library, files.xreading, files.flesch_kincaid_grade, files.flesch_reading_ease FROM files \
    INNER JOIN book_info ON files.book_info_id = book_info.rowid \
    INNER JOIN series ON book_info.series_id = series.rowid \
    INNER JOIN publishers ON series.publisher_id = publishers.rowid \
    INNER JOIN erf_levels ON book_info.erf_level_id = erf_levels.rowid")

conn.commit()

c.execute('SELECT rowid, series_name, publisher, title, fiction_nonfiction, file_name, source FROM meta_data')
database = c.fetchall()

open('Output/meta_data.txt', 'w').write('row_id\tseries_name\tpublisher\ttitle\tfiction_nonfiction\tfile_name\tsource\n')

for row in database:
    output = str(row[0]) + '\t' + str(row[1]) + '\t' + str(row[2]) + '\t' + str(row[3]) + \
        '\t' + str(row[4]) + '\t' + str(row[5]) + '\t' + str(row[6]) + '\n'
    open('Output/meta_data.txt', 'a').write(output)

#############
c.execute('DROP VIEW IF EXISTS erf_database')
conn.commit()

c.execute("CREATE VIEW erf_database AS SELECT book_info.id, series.series_name, publishers.publisher, \
    book_info.title, book_info.publisher_headwords FROM book_info \
    INNER JOIN series ON book_info.series_id = series.rowid \
    INNER JOIN publishers ON series.publisher_id = publishers.rowid")

conn.commit()

c.execute('SELECT * FROM erf_database')
database = c.fetchall()

open('Output/erf_database.txt', 'w').write('book_info.id\tseries_name\tpublisher\ttitle\tpublisher_headwords\n')

for row in database:
    output = str(row[0]) + '\t' + str(row[1]) + '\t' + str(row[2]) + '\t' + str(row[3]) + '\t' + str(row[4]) + '\n'
    open('Output/erf_database.txt', 'a').write(output)



c.execute("UPDATE lexicon SET new_gsl_rank = NULL WHERE new_gsl_rank = '' OR new_gsl_rank = ' '")
c.execute("UPDATE lexicon SET ngsl_rank = NULL WHERE ngsl_rank = '' OR ngsl_rank = ' '")
c.execute("UPDATE lexicon SET sewk_j_rank = NULL WHERE sewk_j_rank = '' OR sewk_j_rank = ' '")
c.execute("UPDATE lexicon SET bnc_coca_rank = NULL WHERE bnc_coca_rank = '' OR bnc_coca_rank = ' '")
conn.commit()


#########
# Views of both corpora that are a bit easier to work with and run analyses
c.execute('DROP VIEW IF EXISTS corpus_view')
conn.commit()
c.execute('CREATE VIEW corpus_view AS SELECT corpus_spacy.rowid, corpus_spacy.file_id, publishers.publisher, series.series_name, \
          book_info.title, book_info.erf_level_id AS erf_sublevel, erf_levels.erf_level_num AS erf_level, \
          book_info.cefr_level_id AS cefr, book_info.yomiyasusa_ave AS yl, files.source, corpus_spacy.type, corpus_spacy.type_lc, \
          corpus_spacy.pos, corpus_spacy.tag, corpus_spacy.pos_tree_conv, corpus_spacy.lemma_ AS lemma, corpus_spacy.lemma_pos, lexicon.family_headword,\
          corpus_spacy.lexicon_id, lexicon.new_gsl_rank, lexicon.ngsl_rank, lexicon.sewk_j_rank, lexicon.bnc_coca_rank FROM corpus_spacy \
            INNER JOIN lexicon ON corpus_spacy.lexicon_id = lexicon.id \
            INNER JOIN files ON corpus_spacy.file_id = files.id \
            INNER JOIN book_info ON files.book_info_id = book_info.id \
            INNER JOIN series ON book_info.series_id = series.id \
            INNER JOIN publishers ON series.publisher_id = publishers.id \
            INNER JOIN erf_levels ON book_info.erf_level_id = erf_levels.id ORDER BY corpus_spacy.rowid')
conn.commit()

######
# MWE views

c.execute("DROP VIEW IF EXISTS two_grams")
c.execute("DROP VIEW IF EXISTS three_grams")
c.execute("DROP VIEW IF EXISTS four_grams")
c.execute("DROP VIEW IF EXISTS five_grams")
conn.commit()


## ISSUES - i want the list to not cross punctuation boundaries - for now i will include them but i could easily write them out
# is it better to leave in some punctuation, though? Like commas, quotes?
# If i want to stop crossing at ONLY periods, i would need to add a "stop" attribute to the corpus_view view
# this will be better than most other methods people use because of this, more meaningful ngrams
# Added a WHERE clause to prevent the ngrams from crossing file boundaries

c.execute("""
CREATE VIEW two_grams
AS 
    SELECT  c1.file_id AS file_id, 
            book_info.erf_level_id AS erf_sublevel,
            book_info.fiction_nonfiction AS f_nf, 
            erf_levels.erf_level_num AS erf_level, 
            c1.type_lc AS type_1,
            c2.type_lc AS type_2,
            c1.lemma_ AS lemma_1, 
            c2.lemma_ AS lemma_2, 
            c1.pos AS pos_1, 
            c2.pos AS pos_2
    FROM corpus_spacy AS c1
        JOIN corpus_spacy AS c2
            ON c1.rowid = c2.rowid - 1
        JOIN files
            ON c1.file_id = files.id
        JOIN book_info
            ON files.book_info_id = book_info.id
        JOIN erf_levels
            ON book_info.erf_level_id = erf_levels.id
    WHERE c1.file_id = c2.file_id
        AND pos_1 NOT IN ('PUNCT', 'SYM')
        AND pos_2 NOT IN ('PUNCT', 'SYM')
""")

c.execute("""
CREATE VIEW three_grams
AS
    SELECT  c1.file_id, 
            book_info.erf_level_id AS erf_sublevel, 
            erf_levels.erf_level_num AS erf_level, 
            c1.type_lc AS type_1,
            c2.type_lc AS type_2,
            c3.type_lc AS type_3,
            c1.lemma_ AS lemma_1, 
            c2.lemma_ AS lemma_2, 
            c3.lemma_ AS lemma_3, 
            c1.pos AS pos_1, 
            c2.pos AS pos_2, 
            c3.pos AS pos_3
    FROM corpus_spacy AS c1
        JOIN corpus_spacy AS c2
            ON c1.rowid = c2.rowid - 1
        JOIN corpus_spacy AS c3
            ON c2.rowid = c3.rowid - 1
        JOIN files
            ON c1.file_id = files.id
        JOIN book_info
            ON files.book_info_id = book_info.id
        JOIN erf_levels
            ON book_info.erf_level_id = erf_levels.id
    WHERE c1.file_id = c3.file_id
        AND pos_1 NOT IN ('PUNCT', 'SYM')
        AND pos_2 NOT IN ('PUNCT', 'SYM')
        AND pos_3 NOT IN ('PUNCT', 'SYM')
""")

c.execute("""
CREATE VIEW four_grams 
AS
    SELECT  c1.file_id, 
            book_info.erf_level_id AS erf_sublevel, 
            erf_levels.erf_level_num AS erf_level, 
            c1.type_lc AS type_1,
            c2.type_lc AS type_2,
            c3.type_lc AS type_3,
            c4.type_lc AS type_4,
            c1.lemma_ AS lemma_1, 
            c2.lemma_ AS lemma_2, 
            c3.lemma_ AS lemma_3, 
            c4.lemma_ AS lemma_4, 
            c1.pos AS pos_1, 
            c2.pos AS pos_2, 
            c3.pos AS pos_3, 
            c4.pos AS pos_4
    FROM corpus_spacy AS c1
        JOIN corpus_spacy AS c2
            ON c1.rowid = c2.rowid - 1
        JOIN corpus_spacy AS c3
            ON c2.rowid = c3.rowid - 1
        JOIN corpus_spacy AS c4
            ON c3.rowid = c4.rowid - 1
        JOIN files
            ON c1.file_id = files.id
        JOIN book_info
            ON files.book_info_id = book_info.id
        JOIN erf_levels
            ON book_info.erf_level_id = erf_levels.id
    WHERE c1.file_id = c4.file_id
        AND pos_1 NOT IN ('PUNCT', 'SYM')
        AND pos_2 NOT IN ('PUNCT', 'SYM')
        AND pos_3 NOT IN ('PUNCT', 'SYM')
        AND pos_4 NOT IN ('PUNCT', 'SYM')
""")

c.execute("""
CREATE VIEW five_grams 
AS
    SELECT  c1.file_id, 
            book_info.erf_level_id AS erf_sublevel, 
            erf_levels.erf_level_num AS erf_level, 
            c1.type_lc AS type_1,
            c2.type_lc AS type_2,
            c3.type_lc AS type_3,
            c4.type_lc AS type_4,
            c5.type_lc AS type_5,
            c1.lemma_ AS lemma_1, 
            c2.lemma_ AS lemma_2, 
            c3.lemma_ AS lemma_3, 
            c4.lemma_ AS lemma_4, 
            c5.lemma_ AS lemma_5, 
            c1.pos AS pos_1, 
            c2.pos AS pos_2, 
            c3.pos AS pos_3, 
            c4.pos AS pos_4, 
            c5.pos AS pos_5
    FROM corpus_spacy AS c1
        JOIN corpus_spacy AS c2
            ON c1.rowid = c2.rowid - 1
        JOIN corpus_spacy AS c3
            ON c2.rowid = c3.rowid - 1
        JOIN corpus_spacy AS c4
            ON c3.rowid = c4.rowid - 1
        JOIN corpus_spacy AS c5
            ON c4.rowid = c5.rowid - 1
        JOIN files
            ON c1.file_id = files.id
        JOIN book_info
            ON files.book_info_id = book_info.id
        JOIN erf_levels
            ON book_info.erf_level_id = erf_levels.id
    WHERE c1.file_id = c5.file_id
        AND pos_1 NOT IN ('PUNCT', 'SYM')
        AND pos_2 NOT IN ('PUNCT', 'SYM')
        AND pos_3 NOT IN ('PUNCT', 'SYM')
        AND pos_4 NOT IN ('PUNCT', 'SYM')
        AND pos_5 NOT IN ('PUNCT', 'SYM')
""")
conn.commit()


#############
# CREATE DESCRIPTIVE TABLES ###
#############

## How many files are there for each publisher at different ERF levels?

with open('Output/Table3_book_count_by_publisher.csv', 'w') as f:
    f.write('publisher,Total,ERF1,ERF2,ERF3,ERF4,ERF5,ERF6,ERF7,ERF8,ERF9,ERF10,\
ERF11,ERF12,ERF13,ERF14,ERF15,ERF16,ERF17,ERF18,ERF19,ERF20\n')

c.execute('SELECT DISTINCT rowid, publisher FROM publishers ORDER BY upper(publisher) ASC')
publishers = c.fetchall()

for pub_id, publisher in publishers:
    # how many files are there for each publisher?
    c.execute('SELECT COUNT(*) FROM meta_data WHERE publisher_id = ?', (pub_id, ))
    total = c.fetchone()
    if total[0] != 0:
        level = 1
        erf_level_dict = {}
        while level <= 20:
            c.execute('SELECT COUNT(*) FROM meta_data WHERE publisher_id = ? AND erf_level_id = ? ', (pub_id, level))
            count = c.fetchone()
            erf_level_dict[level] = count 
            level += 1
        with open('Output/Table3_book_count_by_publisher.csv', 'a') as f:
            f.write(f'{publisher},{total[0]},{erf_level_dict[1][0]},{erf_level_dict[2][0]},\
{erf_level_dict[3][0]},{erf_level_dict[4][0]},{erf_level_dict[5][0]},\
{erf_level_dict[6][0]},{erf_level_dict[7][0]},{erf_level_dict[8][0]},\
{erf_level_dict[9][0]},{erf_level_dict[10][0]},{erf_level_dict[11][0]},\
{erf_level_dict[12][0]},{erf_level_dict[13][0]},{erf_level_dict[14][0]},\
{erf_level_dict[15][0]},{erf_level_dict[16][0]},{erf_level_dict[17][0]},\
{erf_level_dict[18][0]},{erf_level_dict[19][0]},{erf_level_dict[20][0]}\n')
    else:
        continue

# How many files are there for fiction and nonfiction at different ERF levels?

F_NF = ('F', 'NF')
for item in F_NF:
    c.execute('SELECT COUNT (*) FROM meta_data WHERE fiction_nonfiction = ?', (item, ))
    total = c.fetchone()
    level = 1
    erf_level_dict = {}
    while level <= 20:
        c.execute('SELECT COUNT (*) FROM meta_data WHERE fiction_nonfiction = ? AND erf_level_id = ?', (item, level))
        count = c.fetchone()
        erf_level_dict[level] = count 
        level += 1
    with open('Output/Table3_book_count_by_publisher.csv', 'a') as f:
        f.write(f'{item},{total[0]},{erf_level_dict[1][0]},{erf_level_dict[2][0]},\
{erf_level_dict[3][0]},{erf_level_dict[4][0]},{erf_level_dict[5][0]},\
{erf_level_dict[6][0]},{erf_level_dict[7][0]},{erf_level_dict[8][0]},\
{erf_level_dict[9][0]},{erf_level_dict[10][0]},{erf_level_dict[11][0]},\
{erf_level_dict[12][0]},{erf_level_dict[13][0]},{erf_level_dict[14][0]},\
{erf_level_dict[15][0]},{erf_level_dict[16][0]},{erf_level_dict[17][0]},\
{erf_level_dict[18][0]},{erf_level_dict[19][0]},{erf_level_dict[20][0]}\n')

# How many files are there for each source at different ERF levels?

sources = ('xreading', 'scan')
for source in sources:
    c.execute('SELECT COUNT (*) FROM meta_data WHERE source = ?', (source, ))
    total = c.fetchone()
    if total[0] != 0:
        level = 1
        erf_level_dict = {}
        while level <= 20:
            c.execute('SELECT COUNT (*) FROM meta_data WHERE source = ? AND erf_level_id = ?', (source, level))
            count = c.fetchone()
            erf_level_dict[level] = count 
            level += 1
        with open('Output/Table3_book_count_by_publisher.csv', 'a') as f:
            f.write(f'{source},{total[0]},{erf_level_dict[1][0]},{erf_level_dict[2][0]},\
{erf_level_dict[3][0]},{erf_level_dict[4][0]},{erf_level_dict[5][0]},\
{erf_level_dict[6][0]},{erf_level_dict[7][0]},{erf_level_dict[8][0]},\
{erf_level_dict[9][0]},{erf_level_dict[10][0]},{erf_level_dict[11][0]},\
{erf_level_dict[12][0]},{erf_level_dict[13][0]},{erf_level_dict[14][0]},\
{erf_level_dict[15][0]},{erf_level_dict[16][0]},{erf_level_dict[17][0]},\
{erf_level_dict[18][0]},{erf_level_dict[19][0]},{erf_level_dict[20][0]}\n')
    else:
        continue

############  APPENDIX A   #######################

# Produce a table showing the publisher, series, and title for each book followed by its presence in 
# xreading, library, or both, the number of words in the book, the number of standard words, 
# the flesch_kincaid_grade_level, and the flesch_reading_ease score

open("Output/Appendix_A.txt", "w").write("(Location) Publisher, Series, and Title\tWords\tStandard Words\tFlesch Kincaid Grade Level\tFlesch Reading Ease\n")

c.execute("SELECT DISTINCT publisher FROM meta_data ORDER BY upper(publisher) ASC")
publishers = c.fetchall()
for publisher in publishers:
    open("Output/Appendix_A.txt", "a").write(f'{publisher[0]}\n')
    c.execute("SELECT DISTINCT series_name FROM meta_data WHERE publisher = ? ORDER BY upper(series_name) ASC", (publisher[0], ))
    series_names = c.fetchall()
    for series in series_names:
        open("Output/Appendix_A.txt", "a").write(f'\t{series[0]}\n')
        c.execute("SELECT title, library, xreading, character_count, flesch_kincaid_grade, flesch_reading_ease, id \
                FROM meta_data WHERE series_name = ? AND publisher = ?", (series[0], publisher[0] ))
        data = c.fetchall()
        for datum in data:
            query = 'SELECT COUNT(*) FROM corpus_spacy WHERE file_id = ? AND pos != "PUNCT" AND pos != "SYM" AND pos != "CD"'
            tokens = c.execute(query, (datum[6],)).fetchone()[0]
            if datum[1] == 1 and datum[2] == 1:
                presence = "B"
            elif datum[1] == 1:
                presence = "L"
            elif datum[2] == 1:
                presence = "X"
            try:
                standard_words = "%.2f"%(int(datum[3])/6)
            except:
                standard_words = "None"
            open("Output/Appendix_A.txt", "a").write(f'({presence})\t\t{datum[0]}\t{tokens}\t{standard_words}\t{datum[4]}\t{datum[5]}\n')


############  APPENDIX B   #######################
open('Output/Appendix_B.txt', 'w').write('Publisher and Series\t\tBooks in Corpus\tBooks in ERF Lists\t%\n')

book_count_in_corpus = c.execute("SELECT COUNT(*) FROM meta_data").fetchone()[0]
book_count_in_database = c.execute("SELECT COUNT(*) FROM erf_database").fetchone()[0]
book_percent = "%.1f"%((book_count_in_corpus/book_count_in_database)*100)
series_count_in_corpus = c.execute("SELECT COUNT(DISTINCT series_id) FROM meta_data").fetchone()[0]
series_count_in_database = c.execute("SELECT COUNT(DISTINCT series_name) FROM erf_database").fetchone()[0]
series_percent = "%.1f"%((series_count_in_corpus/series_count_in_database)*100)
publisher_count_in_corpus = c.execute("SELECT COUNT(DISTINCT publisher_id) FROM meta_data").fetchone()[0]
publisher_count_in_database = c.execute("SELECT COUNT(DISTINCT publisher) FROM erf_database").fetchone()[0]
publisher_percent = "%.1f"%((publisher_count_in_corpus/publisher_count_in_database)*100)

print('In total, the corpus is made up of {} books, making up {}% of the publishers ({}/{})\
, {}% of the series ({}/{}), and {}% of the book titles ({}/{}) included on the previously \
mentioned ERF lists\n'.format(book_count_in_corpus, publisher_percent,  \
                                publisher_count_in_corpus, publisher_count_in_database, series_percent, \
                                series_count_in_corpus, series_count_in_database, book_percent, \
                                book_count_in_corpus, book_count_in_database))

c.execute("SELECT DISTINCT publisher FROM erf_database ORDER BY upper(publisher) ASC")
publishers = c.fetchall()
for publisher in publishers:
    open("Output/Appendix_B.txt", "a").write(f'{publisher[0]}\n')
    c.execute("SELECT DISTINCT series_name FROM erf_database WHERE publisher = ? ORDER BY upper(series_name) ASC", (publisher[0], ))
    series_names = c.fetchall()
    c.execute("SELECT COUNT(*) FROM erf_database WHERE publisher = ?", (publisher[0], ))
    book_count_in_erf_database_by_publisher = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM meta_data WHERE publisher = ?", (publisher[0], ))
    book_count_in_corpus_by_publisher = c.fetchone()[0]
    publisher_percent = "%.1f"%((book_count_in_corpus_by_publisher/book_count_in_erf_database_by_publisher)*100)
    for series in series_names:
        c.execute("SELECT COUNT(*) FROM erf_database WHERE series_name = ?", (series[0], ))
        book_count_in_erf_database_by_series = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM meta_data WHERE series_name = ?", (series[0], ))
        book_count_in_corpus_by_series = c.fetchone()[0]
        series_percent = "%.1f"%((book_count_in_corpus_by_series/book_count_in_erf_database_by_series)*100)
        output = '\t' + series[0] + '\t' + str(book_count_in_corpus_by_series) + '\t' + str(book_count_in_erf_database_by_series) + '\t' + series_percent + '%\n'
        open("Output/Appendix_B.txt", "a").write(output)
    output = '\tTOTAL\t' + str(book_count_in_corpus_by_publisher) + '\t' + str(book_count_in_erf_database_by_publisher) + '\t' + publisher_percent + '%\n'
    open("Output/Appendix_B.txt", "a").write(output)


c.execute("DROP INDEX IF EXISTS t_index")
c.execute("DROP INDEX IF EXISTS s_index")
conn.commit()

############# DATA SHEET FOR SINGLE WORD ANALYSES ################
c.execute("CREATE INDEX IF NOT EXISTS s_index ON corpus_spacy (file_id, pos)")
conn.commit()

#The script below prevents divide by zero errors
def safe_divide(numerator,denominator): #this function has two arguments
	if denominator == 0: #if the denominator is 0
		output = 0 #the the output is 0
	else: #otherwise
		output = numerator/denominator #the output is the numerator divided by the denominator
	return(output) #return output

def get_text_info(fileid):
    c.execute('SELECT publisher_headwords, yomiyasusa_ave, erf_level_num, erf_level_text, erf_level_id, erf_sublevel_text, publisher, series_name, title FROM meta_data \
              WHERE file_id = ?', (fileid,))
    pub_headword_count, yl_ave, erf_level, erf_level_text, erf_sublevel, erf_sublevel_text, publisher, series, title = c.fetchone()
    return pub_headword_count, yl_ave, erf_level, erf_level_text, erf_sublevel, erf_sublevel_text, publisher, series, title

def word_counts(id, table): # This function has two arguments, fileid and corpus(spacy)
    query = 'SELECT COUNT(DISTINCT type_lc) FROM {} WHERE file_id = ? AND pos NOT IN ("PUNCT", "SYM", "CD") AND tag != "POS"'.format(table)
    types = c.execute(query, (id,)).fetchone()[0]
    query = 'SELECT COUNT(DISTINCT lemma_pos) FROM {} WHERE file_id = ? AND pos NOT IN ("PUNCT", "SYM", "CD") AND tag != "POS"'.format(table)
    lemma_types = c.execute(query, (id,)).fetchone()[0]    
    query = 'SELECT COUNT(DISTINCT lemma) FROM {} WHERE file_id = ? AND pos NOT IN ("PUNCT", "SYM", "CD") AND tag != "POS"'.format(table)
    flemma_types = c.execute(query, (id,)).fetchone()[0]   
    query = 'SELECT COUNT(DISTINCT family_headword) FROM {} WHERE file_id = ? AND pos NOT IN ("PUNCT", "SYM", "CD") AND tag != "POS"'.format(table)
    family_types = c.execute(query, (id,)).fetchone()[0]    
    query = 'SELECT COUNT(*) FROM {} WHERE file_id = ? AND pos != "PUNCT" AND pos NOT IN ("PUNCT", "SYM", "CD") AND tag != "POS"'.format(table)
    tokens = c.execute(query, (id,)).fetchone()[0]    
    query = 'SELECT COUNT(DISTINCT lexicon_id) FROM {} WHERE file_id = ? AND pos = "PROPN"'.format(table)
    proper_noun_types = c.execute(query, (id,)).fetchone()[0]    
    query = 'SELECT COUNT(*) FROM {} WHERE file_id = ? AND pos = "PROPN"'.format(table)
    proper_noun_tokens = c.execute(query, (id,)).fetchone()[0]    
    proper_noun_cov = safe_divide(proper_noun_tokens, tokens)    
    query = 'SELECT COUNT(DISTINCT lexicon_id) FROM {} WHERE file_id = ? AND pos = "INTJ"'.format(table)
    interjection_types = c.execute(query, (id,)).fetchone()[0]    
    query = 'SELECT COUNT(*) FROM {} WHERE file_id = ? AND pos = "INTJ"'.format(table)
    interjection_tokens = c.execute(query, (id,)).fetchone()[0]    
    interjection_cov = safe_divide(interjection_tokens, tokens)

    return types, lemma_types, flemma_types, family_types, tokens, proper_noun_types, proper_noun_tokens, proper_noun_cov, interjection_types, interjection_tokens, interjection_cov


def find_coverage(id, table, word_count):
    lists = ("new_gsl", "ngsl", "sewk_j", "bnc_coca")
    coverage_dict = {}
    for list in lists:
        rank_name = list + "_rank"
        query = "SELECT COUNT(*) FROM {} WHERE file_id = ? AND pos = 'INTJ' AND ({} IS NULL OR {} = '' OR {} =' ')"
        coverage_dict["interjection_" + list] = c.execute(query.format(table, rank_name, rank_name, rank_name), (id,)).fetchone()[0]
        query = "SELECT COUNT(*) FROM {} WHERE file_id = ? AND pos = 'PROPN' AND ({} IS NULL OR {} = '' OR {} =' ')"
        coverage_dict["proper_noun_" + list] = c.execute(query.format(table, rank_name, rank_name, rank_name), (id,)).fetchone()[0]
        query = "SELECT {}, COUNT(*) FROM {} WHERE file_id = ? AND {} IS NOT NULL AND pos NOT IN ('PUNCT', 'SYM', 'CD') AND tag != 'POS' GROUP BY {} ORDER BY {} ASC"
        freq_list = c.execute(query.format(rank_name, table, rank_name, rank_name, rank_name), (id,)).fetchall()  
        cumulative_count = 0    
        cumulative_count_assumed = coverage_dict["proper_noun_" + list] + coverage_dict["interjection_" + list]
        type_counter = 0
        if list == "bnc_coca":
            query = "SELECT COUNT(*) FROM {} WHERE file_id = ? AND bnc_coca_rank IN ('31', '32', '34')"
            cumulative_count_assumed += c.execute(query.format(table), (id,)).fetchone()[0]
        # coverage_dict[list+"_90_rank"] = 0
        coverage_dict[list+"_90_rank_assumed"] = 0
        # coverage_dict[list+"_90_count"] = 0
        coverage_dict[list+"_90_count_assumed"] = 0
        # coverage_dict[list+"_90_cov"] = 0
        coverage_dict[list+"_90_cov_assumed"] = 0     
        # coverage_dict[list+"_95_rank"] = 0
        coverage_dict[list+"_95_rank_assumed"] = 0
        # coverage_dict[list+"_95_count"] = 0
        coverage_dict[list+"_95_count_assumed"] = 0
        # coverage_dict[list+"_95_cov"] = 0
        coverage_dict[list+"_95_cov_assumed"] = 0
        # coverage_dict[list+"_98_rank"] = 0
        coverage_dict[list+"_98_rank_assumed"] = 0
        # coverage_dict[list+"_98_count"] = 0
        coverage_dict[list+"_98_count_assumed"] = 0
        # coverage_dict[list+"_98_cov"] = 0
        coverage_dict[list+"_98_cov_assumed"] = 0      
        coverage_dict[list+"_90_types"] = 0
        coverage_dict[list+"_95_types"] = 0
        coverage_dict[list+"_98_types"] = 0

        for (rank, freq) in freq_list: 
            if rank in ["None", "", " "] or (rank in [31, 32, 34] and list == "bnc_coca"):
                continue
            else:
                # cumulative_count += int(freq)
                cumulative_count_assumed += int(freq) 
                coverage_dict[list+"_max_tokens_assumed"] = cumulative_count_assumed  
                # coverage_dict[list+"_max_percentage"] = safe_divide(cumulative_count, word_count)
                coverage_dict[list+"_max_percentage_assumed"] = safe_divide(cumulative_count_assumed, word_count)
                type_counter += 1
                coverage_dict[list+"_total_on_list_types"] = type_counter
                if safe_divide(cumulative_count_assumed, int(word_count)) >= 0.90 and coverage_dict[list+"_90_rank_assumed"] == 0:
                    coverage_dict[list+"_90_rank_assumed"] = rank
                    coverage_dict[list+"_90_count_assumed"] = cumulative_count_assumed
                    coverage_dict[list+"_90_cov_assumed"] = safe_divide(cumulative_count_assumed, word_count)  
                    coverage_dict[list+"_90_types"] = type_counter                 
                if safe_divide(cumulative_count_assumed, int(word_count)) >= 0.95 and coverage_dict[list+"_95_rank_assumed"] == 0:
                    coverage_dict[list+"_95_rank_assumed"] = rank
                    coverage_dict[list+"_95_count_assumed"] = cumulative_count_assumed
                    coverage_dict[list+"_95_cov_assumed"] = safe_divide(cumulative_count_assumed, word_count)       
                    coverage_dict[list+"_95_types"] = type_counter                                   
                if safe_divide(cumulative_count_assumed, int(word_count)) >= 0.98 and coverage_dict[list+"_98_rank_assumed"] == 0:
                    coverage_dict[list+"_98_rank_assumed"] = rank
                    coverage_dict[list+"_98_count_assumed"] = cumulative_count_assumed
                    coverage_dict[list+"_98_cov_assumed"] = safe_divide(cumulative_count_assumed, word_count)
                    coverage_dict[list+"_98_types"] = type_counter                                     
                # if safe_divide(cumulative_count, int(word_count)) >= 0.90 and coverage_dict[list+"_90_rank"] == 0:
                #     coverage_dict[list+"_90_rank"] = rank
                #     coverage_dict[list+"_90_count"] = cumulative_count
                #     coverage_dict[list+"_90_cov"] = safe_divide(cumulative_count, word_count)
                # if safe_divide(cumulative_count, int(word_count)) >= 0.95 and coverage_dict[list+"_95_rank"] == 0:
                #     coverage_dict[list+"_95_rank"] = rank
                #     coverage_dict[list+"_95_count"] = cumulative_count
                #     coverage_dict[list+"_95_cov"] = safe_divide(cumulative_count, word_count)
                # if safe_divide(cumulative_count, int(word_count)) >= 0.98 and coverage_dict[list+"_98_rank"] == 0:
                #     coverage_dict[list+"_98_rank"] = rank
                #     coverage_dict[list+"_98_count"] = cumulative_count
                #     coverage_dict[list+"_98_cov"] = safe_divide(cumulative_count, word_count)

    # These below are meant to count how many off-list words could be added to reach each coverage level (in frequency order)
    query = "SELECT DISTINCT lemma_pos, COUNT(*) AS c FROM {} WHERE file_id = ? AND (new_gsl_rank IS NULL OR new_gsl_rank = '' OR new_gsl_rank =' ')\
          AND pos NOT IN ('PROPN', 'INTJ', 'PUNCT', 'SYM', 'CD') AND tag != 'POS' GROUP BY lemma_pos ORDER BY c DESC"
    off_list_freq = c.execute(query.format(table), (id,)).fetchall() 
    off_list_counter = 0
    off_list_coverage = coverage_dict["new_gsl_max_tokens_assumed"]
    coverage_dict["off_list_new_gsl_for_90"] = 0
    coverage_dict["off_list_new_gsl_for_95"] = 0
    coverage_dict["off_list_new_gsl_for_98"] = 0
    for (grouping, freq) in off_list_freq:
        off_list_counter += 1
        off_list_coverage += int(freq)
        if safe_divide(off_list_coverage, int(word_count)) >= 0.90 and coverage_dict["new_gsl_90_rank_assumed"] == 0:
            coverage_dict["off_list_new_gsl_for_90"] = off_list_counter
            coverage_dict["new_gsl_90_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.95 and coverage_dict["new_gsl_95_rank_assumed"] == 0:
            coverage_dict["off_list_new_gsl_for_95"] = off_list_counter
            coverage_dict["new_gsl_95_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.98 and coverage_dict["new_gsl_98_rank_assumed"] == 0:
            coverage_dict["off_list_new_gsl_for_98"] = off_list_counter
            coverage_dict["new_gsl_98_rank_assumed"] = "NA"
    
    query = "SELECT DISTINCT lemma, COUNT(*) AS c FROM {} WHERE file_id = ? AND (ngsl_rank IS NULL OR ngsl_rank = '' OR ngsl_rank =' ')\
          AND pos NOT IN ('PROPN', 'INTJ', 'PUNCT', 'SYM', 'CD') AND tag != 'POS' GROUP BY lemma ORDER BY c DESC"
    off_list_freq = c.execute(query.format(table), (id,)).fetchall() 
    off_list_counter = 0
    off_list_coverage = coverage_dict["ngsl_max_tokens_assumed"]
    coverage_dict["off_list_ngsl_for_90"] = 0
    coverage_dict["off_list_ngsl_for_95"] = 0
    coverage_dict["off_list_ngsl_for_98"] = 0
    for (grouping, freq) in off_list_freq:
        off_list_counter += 1
        off_list_coverage += int(freq)
        if safe_divide(off_list_coverage, int(word_count)) >= 0.90 and coverage_dict["ngsl_90_rank_assumed"] == 0:
            coverage_dict["off_list_ngsl_for_90"] = off_list_counter
            coverage_dict["ngsl_90_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.95 and coverage_dict["ngsl_95_rank_assumed"] == 0:
            coverage_dict["off_list_ngsl_for_95"] = off_list_counter
            coverage_dict["ngsl_95_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.98 and coverage_dict["ngsl_98_rank_assumed"] == 0:
            coverage_dict["off_list_ngsl_for_98"] = off_list_counter
            coverage_dict["ngsl_98_rank_assumed"] = "NA"

    query = "SELECT DISTINCT lemma, COUNT(*) AS c FROM {} WHERE file_id = ? AND (sewk_j_rank IS NULL OR sewk_j_rank = '' OR sewk_j_rank =' ') \
        AND pos NOT IN ('PROPN', 'INTJ', 'PUNCT', 'SYM', 'CD') AND tag != 'POS' GROUP BY lemma ORDER BY c DESC"
    off_list_freq = c.execute(query.format(table), (id,)).fetchall() 
    off_list_counter = 0
    off_list_coverage = coverage_dict["sewk_j_max_tokens_assumed"]
    coverage_dict["off_list_sewk_j_for_90"] = 0
    coverage_dict["off_list_sewk_j_for_95"] = 0
    coverage_dict["off_list_sewk_j_for_98"] = 0
    for (grouping, freq) in off_list_freq:
        off_list_counter += 1
        off_list_coverage += int(freq)
        if safe_divide(off_list_coverage, int(word_count)) >= 0.90 and coverage_dict["sewk_j_90_rank_assumed"] == 0:
            coverage_dict["off_list_sewk_j_for_90"] = off_list_counter
            coverage_dict["sewk_j_90_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.95 and coverage_dict["sewk_j_95_rank_assumed"] == 0:
            coverage_dict["off_list_sewk_j_for_95"] = off_list_counter
            coverage_dict["sewk_j_95_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.98 and coverage_dict["sewk_j_98_rank_assumed"] == 0:
            coverage_dict["off_list_sewk_j_for_98"] = off_list_counter
            coverage_dict["sewk_j_98_rank_assumed"] = "NA"
    
    query = "SELECT DISTINCT family_headword, COUNT(*) AS c FROM {} WHERE file_id = ? AND (bnc_coca_rank IS NULL OR bnc_coca_rank = '' OR bnc_coca_rank =' ') \
        AND pos NOT IN ('PROPN', 'INTJ', 'PUNCT', 'SYM', 'CD') AND tag != 'POS' GROUP BY family_headword ORDER BY c DESC"
    off_list_freq = c.execute(query.format(table), (id,)).fetchall() 
    off_list_counter = 0
    off_list_coverage = coverage_dict["bnc_coca_max_tokens_assumed"]
    coverage_dict["off_list_bnc_coca_for_90"] = 0
    coverage_dict["off_list_bnc_coca_for_95"] = 0
    coverage_dict["off_list_bnc_coca_for_98"] = 0
    for (grouping, freq) in off_list_freq:
        off_list_counter += 1
        off_list_coverage += int(freq)
        if safe_divide(off_list_coverage, int(word_count)) >= 0.90 and coverage_dict["bnc_coca_90_rank_assumed"] == 0:
            coverage_dict["off_list_bnc_coca_for_90"] = off_list_counter
            coverage_dict["bnc_coca_90_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.95 and coverage_dict["bnc_coca_95_rank_assumed"] == 0:
            coverage_dict["off_list_bnc_coca_for_95"] = off_list_counter
            coverage_dict["bnc_coca_95_rank_assumed"] = "NA"
        if safe_divide(off_list_coverage, int(word_count)) >= 0.98 and coverage_dict["bnc_coca_98_rank_assumed"] == 0:
            coverage_dict["off_list_bnc_coca_for_98"] = off_list_counter
            coverage_dict["bnc_coca_98_rank_assumed"] = "NA"
    
# The sections below are intended to be a pure frequency list for each book using the different counting methods
    unit = "lemma"
    coverage_dict[unit + "_90"] = 0
    coverage_dict[unit + "_95"] = 0
    coverage_dict[unit + "_98"] = 0
    query = "SELECT COUNT(*) FROM {} WHERE file_id = ? AND (pos = 'INTJ' OR pos = 'PROPN')"
    inter_and_propn = c.execute(query.format(table), (id,)).fetchone()[0]
    cumulative_coverage = inter_and_propn
    units_required = 1
    # for this file_id, extract each distinct lemma_pos and count how many there are
    for (lemma_pos, count) in c.execute("SELECT DISTINCT lemma_pos, COUNT(*) AS freq FROM {} WHERE file_id = ? \
                                        AND pos NOT IN ('INTJ', 'PROPN', 'PUNCT', 'SYM', 'CD') AND tag != 'POS' \
                                        GROUP BY lemma_pos ORDER BY freq DESC".format(table), (id,)).fetchall():
        cumulative_coverage += count
        if safe_divide(cumulative_coverage, int(word_count)) >= 0.90 and coverage_dict[unit + "_90"] == 0:
            coverage_dict[unit + "_90"] = units_required
        if safe_divide(cumulative_coverage, int(word_count)) >= 0.95 and coverage_dict[unit + "_95"] == 0:
            coverage_dict[unit + "_95"] = units_required
        if safe_divide(cumulative_coverage, int(word_count)) >= 0.98 and coverage_dict[unit + "_98"] == 0:
            coverage_dict[unit + "_98"] = units_required
        units_required += 1
        
    unit = "flemma"
    coverage_dict[unit + "_90"] = 0
    coverage_dict[unit + "_95"] = 0
    coverage_dict[unit + "_98"] = 0
    cumulative_coverage = inter_and_propn
    units_required = 1
    # for this file_id, extract each distinct lemma_ and count how many there are
    for (lemma_, count) in c.execute("SELECT DISTINCT lemma, COUNT(*) AS freq FROM {} WHERE file_id = ? \
                                     AND pos NOT IN ('INTJ', 'PROPN', 'PUNCT', 'SYM', 'CD') AND tag != 'POS' \
                                     GROUP BY lemma ORDER BY freq DESC".format(table), (id,)).fetchall():
        cumulative_coverage += count
        if safe_divide(cumulative_coverage, int(word_count)) >= 0.90 and coverage_dict[unit + "_90"] == 0:
            coverage_dict[unit + "_90"] = units_required
        if safe_divide(cumulative_coverage, int(word_count)) >= 0.95 and coverage_dict[unit + "_95"] == 0:
            coverage_dict[unit + "_95"] = units_required
        if safe_divide(cumulative_coverage, word_count) >= 0.98 and coverage_dict[unit + "_98"] == 0:
            coverage_dict[unit + "_98"] = units_required
        units_required += 1
    
    unit = "family"
    coverage_dict[unit + "_90"] = 0
    coverage_dict[unit + "_95"] = 0
    coverage_dict[unit + "_98"] = 0
    cumulative_coverage = inter_and_propn
    units_required = 1
    # for this file_id, extract each distinct family_headword and count how many there are
    for (family_headword, count) in c.execute("SELECT DISTINCT family_headword, COUNT(*) AS freq FROM {} \
                                              WHERE pos NOT IN ('INTJ', 'PROPN', 'PUNCT', 'SYM', 'CD') AND tag != 'POS' \
                                              AND file_id = ? GROUP BY family_headword ORDER BY freq DESC".format(table), (id,)).fetchall():
        cumulative_coverage += count
        if safe_divide(cumulative_coverage, int(word_count)) >= 0.90 and coverage_dict[unit + "_90"] == 0:
            coverage_dict[unit + "_90"] = units_required
        if safe_divide(cumulative_coverage, int(word_count)) >= 0.95 and coverage_dict[unit + "_95"] == 0:
            coverage_dict[unit + "_95"] = units_required
        if safe_divide(cumulative_coverage, word_count) >= 0.98 and coverage_dict[unit + "_98"] == 0:
            coverage_dict[unit + "_98"] = units_required
        units_required += 1

    return coverage_dict  # A dictionary of the coverage values for each list    

# the categories such as interjection_ngsl and proper_noun_ngsl are meant to count the number of tokens in those
# categories that are not in the ngsl list.  This is so they can be added afterwards to the total number of tokens
output_headings = 'file_id  ,\
yl_ave  ,\
erf_level  ,\
erf_level_text  ,\
erf_sublevel  ,\
erf_sublevel_text  ,\
publisher_headword_count  ,\
publisher  ,\
series  ,\
title  ,\
token_count  ,\
type_count  ,\
lemma_type_count  ,\
flemma_type_count  ,\
family_type_count  ,\
lemma_90  ,\
flemma_90  ,\
family_90  ,\
lemma_95  ,\
flemma_95  ,\
family_95  ,\
lemma_98  ,\
flemma_98  ,\
family_98  ,\
proper_noun_types  ,\
proper_noun_tokens  ,\
proper_noun_cov%  ,\
interjection_types  ,\
interjection_count  ,\
interjection_cov%  ,\
interjection_new_gsl  ,\
proper_noun_new_gsl  ,\
new_gsl_max_cov_assumed  ,\
new_gsl_total_on_list_types  ,\
new_gsl_90_rank_assumed  ,\
new_gsl_90_count_assumed  ,\
new_gsl_90_cov_assumed  ,\
new_gsl_90_types  ,\
new_gsl_90_off_list_types_needed  ,\
new_gsl_95_rank_assumed  ,\
new_gsl_95_count_assumed  ,\
new_gsl_95_cov_assumed  ,\
new_gsl_95_types  ,\
new_gsl_95_off_list_types_needed  ,\
new_gsl_98_rank_assumed  ,\
new_gsl_98_count_assumed  ,\
new_gsl_98_cov_assumed  ,\
new_gsl_98_types  ,\
new_gsl_98_off_list_types_needed  ,\
interjection_ngsl  ,\
proper_noun_ngsl  ,\
ngsl_total_on_list_types  ,\
ngsl_90_rank_assumed  ,\
ngsl_90_count_assumed  ,\
ngsl_90_cov_assumed  ,\
ngsl_90_types  ,\
ngsl_90_off_list_types_needed  ,\
ngsl_95_rank_assumed  ,\
ngsl_95_count_assumed  ,\
ngsl_95_cov_assumed  ,\
ngsl_95_types  ,\
ngsl_95_off_list_types_needed  ,\
ngsl_98_rank_assumed  ,\
ngsl_98_count_assumed  ,\
ngsl_98_cov_assumed  ,\
ngsl_98_types  ,\
ngsl_98_off_list_types_needed  ,\
interjection_sewk_j  ,\
proper_noun_sewk_j  ,\
sewk_j_total_on_list_types  ,\
sewk_j_90_rank_assumed  ,\
sewk_j_90_count_assumed  ,\
sewk_j_90_cov_assumed  ,\
sewk_j_90_types  ,\
sewk_j_90_off_list_types_needed  ,\
sewk_j_95_rank_assumed  ,\
sewk_j_95_count_assumed  ,\
sewk_j_95_cov_assumed  ,\
sewk_j_95_types  ,\
sewk_j_95_off_list_types_needed  ,\
sewk_j_98_rank_assumed  ,\
sewk_j_98_count_assumed  ,\
sewk_j_98_cov_assumed  ,\
sewk_j_98_types  ,\
sewk_j_98_off_list_types_needed  ,\
interjection_bnc_coca  ,\
proper_noun_bnc_coca  ,\
bnc_coca_total_on_list_types  ,\
bnc_coca_90_rank_assumed  ,\
bnc_coca_90_count_assumed  ,\
bnc_coca_90_cov_assumed  ,\
bnc_coca_90_types  ,\
bnc_coca_90_off_list_types_needed  ,\
bnc_coca_95_rank_assumed  ,\
bnc_coca_95_count_assumed  ,\
bnc_coca_95_cov_assumed  ,\
bnc_coca_95_types  ,\
bnc_coca_95_off_list_types_needed  ,\
bnc_coca_98_rank_assumed  ,\
bnc_coca_98_count_assumed  ,\
bnc_coca_98_cov_assumed  ,\
bnc_coca_98_types  ,\
bnc_coca_98_off_list_types_needed  \n'


output_file = 'Output/1_single_word_data_spacy.csv'
view_name = 'corpus_view'
with open(output_file, 'w') as f:
    f.write(output_headings)

# Extract a list of all unique file_ids from the database
query = 'SELECT DISTINCT file_id FROM {} ORDER BY file_id'.format(view_name)
file_ids = c.execute(query).fetchall()

# Create a loop to go through all of the files in the files table of the database
for fileid in file_ids:
    fileid = fileid[0]
    pub_headword_count, yl_ave, erf_level, erf_level_text, erf_sublevel, erf_sublevel_text, publisher, series, title = get_text_info(fileid)
    if type(pub_headword_count) != int and pub_headword_count != '':
        pub_headword_count = pub_headword_count.replace(',', '')
        pub_headword_count = pub_headword_count.replace(' ', '')
        pub_headword_count = int(pub_headword_count)
    title = title.replace(',', '')
    types, lemma_types, flemma_types, family_types, tokens, proper_noun_types, proper_noun_tokens, proper_noun_cov, \
        interjection_types, interjection_tokens, interjection_cov = word_counts(fileid, view_name)
    coverage_dict = find_coverage(fileid, view_name, tokens)

#append csv file to store the results
    with open(output_file, 'a') as f:
        f.write(f'{fileid},\
        {yl_ave},\
        {erf_level},\
        {erf_level_text},\
        {erf_sublevel},\
        {erf_sublevel_text},\
        {pub_headword_count},\
        {publisher},\
        {series},\
        {title},\
        {tokens},\
        {types},\
        {lemma_types},\
        {flemma_types},\
        {family_types},\
        {coverage_dict["lemma_90"]},\
        {coverage_dict["flemma_90"]},\
        {coverage_dict["family_90"]},\
        {coverage_dict["lemma_95"]},\
        {coverage_dict["flemma_95"]},\
        {coverage_dict["family_95"]},\
        {coverage_dict["lemma_98"]},\
        {coverage_dict["flemma_98"]},\
        {coverage_dict["family_98"]},\
        {proper_noun_types},\
        {proper_noun_tokens},\
        {proper_noun_cov},\
        {interjection_types},\
        {interjection_tokens},\
        {interjection_cov},\
        {coverage_dict["interjection_new_gsl"]},\
        {coverage_dict["proper_noun_new_gsl"]},\
        {coverage_dict["new_gsl_max_percentage_assumed"]},\
        {coverage_dict["new_gsl_total_on_list_types"]},\
        {coverage_dict["new_gsl_90_rank_assumed"]},\
        {coverage_dict["new_gsl_90_count_assumed"]},\
        {coverage_dict["new_gsl_90_cov_assumed"]},\
        {coverage_dict["new_gsl_90_types"]},\
        {coverage_dict["off_list_new_gsl_for_90"]},\
        {coverage_dict["new_gsl_95_rank_assumed"]},\
        {coverage_dict["new_gsl_95_count_assumed"]},\
        {coverage_dict["new_gsl_95_cov_assumed"]},\
        {coverage_dict["new_gsl_95_types"]},\
        {coverage_dict["off_list_new_gsl_for_95"]},\
        {coverage_dict["new_gsl_98_rank_assumed"]},\
        {coverage_dict["new_gsl_98_count_assumed"]},\
        {coverage_dict["new_gsl_98_cov_assumed"]},\
        {coverage_dict["new_gsl_98_types"]},\
        {coverage_dict["off_list_new_gsl_for_98"]},\
        {coverage_dict["interjection_ngsl"]},\
        {coverage_dict["proper_noun_ngsl"]},\
        {coverage_dict["ngsl_total_on_list_types"]},\
        {coverage_dict["ngsl_90_rank_assumed"]},\
        {coverage_dict["ngsl_90_count_assumed"]},\
        {coverage_dict["ngsl_90_cov_assumed"]},\
        {coverage_dict["ngsl_90_types"]},\
        {coverage_dict["off_list_ngsl_for_90"]},\
        {coverage_dict["ngsl_95_rank_assumed"]},\
        {coverage_dict["ngsl_95_count_assumed"]},\
        {coverage_dict["ngsl_95_cov_assumed"]},\
        {coverage_dict["ngsl_95_types"]},\
        {coverage_dict["off_list_ngsl_for_95"]},\
        {coverage_dict["ngsl_98_rank_assumed"]},\
        {coverage_dict["ngsl_98_count_assumed"]},\
        {coverage_dict["ngsl_98_cov_assumed"]},\
        {coverage_dict["ngsl_98_types"]},\
        {coverage_dict["off_list_ngsl_for_98"]},\
        {coverage_dict["interjection_sewk_j"]},\
        {coverage_dict["proper_noun_sewk_j"]},\
        {coverage_dict["sewk_j_total_on_list_types"]},\
        {coverage_dict["sewk_j_90_rank_assumed"]},\
        {coverage_dict["sewk_j_90_count_assumed"]},\
        {coverage_dict["sewk_j_90_cov_assumed"]},\
        {coverage_dict["sewk_j_90_types"]},\
        {coverage_dict["off_list_sewk_j_for_90"]},\
        {coverage_dict["sewk_j_95_rank_assumed"]},\
        {coverage_dict["sewk_j_95_count_assumed"]},\
        {coverage_dict["sewk_j_95_cov_assumed"]},\
        {coverage_dict["sewk_j_95_types"]},\
        {coverage_dict["off_list_sewk_j_for_95"]},\
        {coverage_dict["sewk_j_98_rank_assumed"]},\
        {coverage_dict["sewk_j_98_count_assumed"]},\
        {coverage_dict["sewk_j_98_cov_assumed"]},\
        {coverage_dict["sewk_j_98_types"]},\
        {coverage_dict["off_list_sewk_j_for_98"]},\
        {coverage_dict["interjection_bnc_coca"]},\
        {coverage_dict["proper_noun_bnc_coca"]},\
        {coverage_dict["bnc_coca_total_on_list_types"]},\
        {coverage_dict["bnc_coca_90_rank_assumed"]},\
        {coverage_dict["bnc_coca_90_count_assumed"]},\
        {coverage_dict["bnc_coca_90_cov_assumed"]},\
        {coverage_dict["bnc_coca_90_types"]},\
        {coverage_dict["off_list_bnc_coca_for_90"]},\
        {coverage_dict["bnc_coca_95_rank_assumed"]},\
        {coverage_dict["bnc_coca_95_count_assumed"]},\
        {coverage_dict["bnc_coca_95_cov_assumed"]},\
        {coverage_dict["bnc_coca_95_types"]},\
        {coverage_dict["off_list_bnc_coca_for_95"]},\
        {coverage_dict["bnc_coca_98_rank_assumed"]},\
        {coverage_dict["bnc_coca_98_count_assumed"]},\
        {coverage_dict["bnc_coca_98_cov_assumed"]},\
        {coverage_dict["bnc_coca_98_types"]},\
        {coverage_dict["off_list_bnc_coca_for_98"]}\n')

    clean_up = open(output_file, 'r').read()
    clean_up = clean_up.replace('            ', '')
    open(output_file, 'w').write(clean_up)
c.execute("DROP INDEX IF EXISTS s_index")
conn.commit()

conn.close()

end_time = time.time()
print("Elapsed time was %g seconds" % (end_time - start_time))
print("In minutes: %g" % ((end_time - start_time)/60))