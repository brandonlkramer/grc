# Results sheet for Research Question 7 - PHRASE LIST

import sqlite3, os

# connect to the database
if os.name == 'nt':  # 'nt' is the name for Windows in Python's os module
    grc_conn = sqlite3.connect('D:\\graded_readers_mwe.sqlite')
else:  # Assume Unix-based system (like macOS or Linux)
    grc_conn = sqlite3.connect('/Volumes/T7/graded_readers_mwe.sqlite')

grc_c = grc_conn.cursor() # Creates a cursor

#########################################

erf_levels = ((1,"one"),(2,"two"),(3,"three"),(4,"four"),(5,"five"),(6,"six"))
output_headings = "PHRASE Item, ERF1_freq, ERF2_freq, ERF3_freq, ERF4_freq, ERF5_freq, ERF6_freq, GRC_total, \
fic_ERF1_freq, fic_ERF2_freq, fic_ERF3_freq, fic_ERF4_freq, fic_ERF5_freq, fic_ERF6_freq, GRC_fic_total\n"

# open the phave file
phrase_file = os.path.join('@_database_setup_uploads', 'phrase.txt')
with open(phrase_file, 'r', encoding='utf-8') as f:
    phrase_list = f.readlines() 

output_file = os.path.join('Output', 'RQ8_phrase_frequency.csv')
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(output_headings)
    missing_words = []
    for line in phrase_list:
        erf_one_freq = 0
        erf_two_freq = 0
        erf_three_freq = 0
        erf_four_freq = 0
        erf_five_freq = 0
        erf_six_freq = 0
        total_grc_freq = 0
        erf_one_fic_freq = 0
        erf_two_fic_freq = 0
        erf_three_fic_freq = 0
        erf_four_fic_freq = 0
        erf_five_fic_freq = 0
        erf_six_fic_freq = 0
        total_grc_fic_freq = 0
        phrase_line = line.split('\t')
        rank = phrase_line[0]
        search_terms = phrase_line[1]
        ngram_size = int(phrase_line[2])
        phrase_text = phrase_line[3]
        wordone = phrase_line[4].lower()
        wordoneID = grc_c.execute("SELECT ID FROM lexicon WHERE type_lc = ?", (wordone,)).fetchone()[0]
        wordtwo = phrase_line[5].lower()
        wordtwoID = grc_c.execute("SELECT ID FROM lexicon WHERE type_lc = ?", (wordtwo,)).fetchone()[0]
        if search_terms >= '3':
            wordthree = phrase_line[6].lower()
            wordthreeID = grc_c.execute("SELECT ID FROM lexicon WHERE type_lc = ?", (wordthree,)).fetchone()[0]
        if search_terms == '4':
            wordfour = phrase_line[7].lower()
            wordfourID = grc_c.execute("SELECT ID FROM lexicon WHERE type_lc = ?", (wordfour,)).fetchone()[0]
        query_string = phrase_line[8]
        query_string = query_string.strip('\n')
        #print(query_string)

        query = "SELECT total_grc_freq, total_grc_fic_freq, erf_one_freq, erf_two_freq, erf_three_freq, \
            erf_four_freq, erf_five_freq, erf_six_freq, erf_one_fic_freq, erf_two_fic_freq, erf_three_fic_freq, erf_four_fic_freq, \
            erf_five_fic_freq, erf_six_fic_freq \
            FROM ngrams \
            WHERE ngram_size = ? AND " + query_string
        
        if search_terms == '2':
            grc_c.execute(query, (ngram_size, wordoneID, wordtwoID))
        elif search_terms == '3':
            grc_c.execute(query, (ngram_size, wordoneID, wordtwoID, wordthreeID))
        elif search_terms == '4':
            grc_c.execute(query, (ngram_size, wordoneID, wordtwoID, wordthreeID, wordfourID))

        matches = grc_c.fetchall()
        try:
            for match in matches:
                total_grc_freq += match[0]
                total_grc_fic_freq += match[1]
                erf_one_freq += match[2]
                erf_two_freq += match[3]
                erf_three_freq += match[4]
                erf_four_freq += match[5]
                erf_five_freq += match[6]
                erf_six_freq += match[7]
                erf_one_fic_freq += match[8]
                erf_two_fic_freq += match[9]
                erf_three_fic_freq += match[10]
                erf_four_fic_freq += match[11]
                erf_five_fic_freq += match[12]
                erf_six_fic_freq += match[13]
        except:
            missing_words.append(phrase_text)
            continue
        rank_word = str(rank) + ' ' + phrase_text
        f.write(rank_word + ',' + str(erf_one_freq) + ',' + str(erf_two_freq) + \
        ',' + str(erf_three_freq) + ',' + str(erf_four_freq) + ',' + str(erf_five_freq) + ',' + str(erf_six_freq) + ',' + str(total_grc_freq) + \
        ',' + str(erf_one_fic_freq) + ',' + str(erf_two_fic_freq) + ',' + str(erf_three_fic_freq) + ',' + str(erf_four_fic_freq) + ',' + str(erf_five_fic_freq) + \
        ',' + str(erf_six_fic_freq) + ',' + str(total_grc_fic_freq) + '\n')

print(len(missing_words), "PHRASE items were not in the GRC database")
for word in missing_words:
    print(word)

grc_conn.commit()
grc_conn.close()