# Results sheet for Research Question 8 - PHAVE LIST

import sqlite3, os

# connect to the database
if os.name == 'nt':  # 'nt' is the name for Windows in Python's os module
    grc_conn = sqlite3.connect('D:\\graded_readers_mwe.sqlite')
else:  # Assume Unix-based system (like macOS or Linux)
    grc_conn = sqlite3.connect('/Volumes/T7/graded_readers_mwe.sqlite')

grc_c = grc_conn.cursor() # Creates a cursor

grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_IN ON ngrams (ngram_size, total_grc_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_fic_IN ON ngrams (ngram_size, total_grc_fic_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_one_IN ON ngrams (ngram_size, erf_one_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_two_IN ON ngrams (ngram_size, erf_two_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_three_IN ON ngrams (ngram_size, erf_three_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_four_IN ON ngrams (ngram_size, erf_four_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_five_IN ON ngrams (ngram_size, erf_five_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_six_IN ON ngrams (ngram_size, erf_six_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_one_fic_IN ON ngrams (ngram_size, erf_one_fic_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_two_fic_IN ON ngrams (ngram_size, erf_two_fic_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_three_fic_IN ON ngrams (ngram_size, erf_three_fic_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_four_fic_IN ON ngrams (ngram_size, erf_four_fic_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_five_fic_IN ON ngrams (ngram_size, erf_five_fic_freq)")
grc_c.execute("CREATE INDEX IF NOT EXISTS ngram_count_erf_six_fic_IN ON ngrams (ngram_size, erf_six_fic_freq)")
grc_conn.commit()

#########################################

erf_levels = ((1,"one"),(2,"two"),(3,"three"),(4,"four"),(5,"five"),(6,"six"))
output_headings = "Phave Item, ERF1_freq, ERF2_freq, ERF3_freq, ERF4_freq, ERF5_freq, ERF6_freq, GRC_total, \
fic_ERF1_freq, fic_ERF2_freq, fic_ERF3_freq, fic_ERF4_freq, fic_ERF5_freq, fic_ERF6_freq, GRC_fic_total\n"

# open the phave file
phave_file = os.path.join('@_database_setup_uploads', 'phave.txt')
with open(phave_file, 'r', encoding='utf-8') as f:
    phave_list = f.readlines() 

output_file = os.path.join('Output', 'RQ9_phave_frequency.csv')
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(output_headings)
    rank = 1
    missing_words = []
    for word in phave_list:
        wordone = word.split()[0].lower()
        wordtwo = word.split()[1].lower()
        word = word.strip('\n')
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
        # First I want to write the info for the lemma as a whole
        grc_c.execute("SELECT total_grc_freq, total_grc_fic_freq, erf_one_freq, erf_two_freq, erf_three_freq, \
            erf_four_freq, erf_five_freq, erf_six_freq, erf_one_fic_freq, erf_two_fic_freq, erf_three_fic_freq, erf_four_fic_freq, \
            erf_five_fic_freq, erf_six_fic_freq, wordoneID \
            FROM ngrams \
            WHERE ngram_size = 2 AND lemmaoneID = (SELECT ID FROM lexicon WHERE type_lc = ?) \
            AND wordtwoID = (SELECT ID FROM lexicon WHERE type_lc = ?)", (wordone, wordtwo))


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
            missing_words.append(word)
            continue
        rank_word = str(rank) + ' ' + word
        f.write(rank_word + ',' + str(erf_one_freq) + ',' + str(erf_two_freq) + \
        ',' + str(erf_three_freq) + ',' + str(erf_four_freq) + ',' + str(erf_five_freq) + ',' + str(erf_six_freq) + ',' + str(total_grc_freq) + \
        ',' + str(erf_one_fic_freq) + ',' + str(erf_two_fic_freq) + ',' + str(erf_three_fic_freq) + ',' + str(erf_four_fic_freq) + ',' + str(erf_five_fic_freq) + \
        ',' + str(erf_six_fic_freq) + ',' + str(total_grc_fic_freq) + '\n')
        rank += 1

        # Next I want to write the info for each inflection
        try:
            for match in matches:
                grc_c.execute("SELECT type_lc FROM lexicon WHERE ID = ?", (match[14],))
                exact_type = grc_c.fetchone()[0]
                exact_ngram = exact_type + ' ' + wordtwo
                f.write('   ' + exact_ngram + ',' + str(match[2]) + ',' + str(match[3]) + \
                ',' + str(match[4]) + ',' + str(match[5]) + ',' + str(match[6]) + ',' + str(match[7]) + ',' + str(match[0]) + \
                ',' + str(match[8]) + ',' + str(match[9]) + ',' + str(match[10]) + ',' + str(match[11]) + ',' + str(match[12]) + \
                ',' + str(match[13]) + ',' + str(match[1]) + '\n')
        except:
            pass

print(len(missing_words), "phave items were not in the GRC database")
for word in missing_words:
    print(word)

grc_conn.commit()
grc_conn.close()