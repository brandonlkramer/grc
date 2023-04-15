import glob, re
from wordsegment import load, segment

load()

word_list = []
list = open('sewk_j.txt', 'r').read().splitlines()
for line in list:
    line = line.split('\t')
    word_list.append(line[0].upper())

proper_nouns = []
list = open('bnc_coca_proper_nouns.txt', 'r').read().splitlines()
for line in list:
    line = line.split('\t')
    proper_nouns.append(line[0].upper())

interjections = []
list = open('bnc_coca_interjections.txt', 'r').read().splitlines()
for line in list:
    line = line.split('\t')
    proper_nouns.append(line[0].upper())

list = open('errors_segmenter.txt', 'r').read().splitlines()
suggestions_made = {}
for line in list:
    line = line.split('\t')
    suggestions_made[line[0]] = line[1]

def replace_periods_in_ellipsis(text):
    return re.sub(r'(?<=\w)\.{2,}(?=\w)', ' ... ', text)

def add_space_after_punctuation(text):
    return re.sub(r'(?<=[.,?!])(?=\w)', ' ', text)

def is_time(word):
    pattern = r'\d{1,2}:\d{2}(?:[ap]\\.?m\\.?)?$'
    return bool(re.match(pattern, word))

def is_special_pattern(word):
    pattern = r'\d+(?:km|th|rd|nd)$'
    return bool(re.match(pattern, word))

def process_word(word, tokens):
#    print(f"Processing word: {word}")
    if not word:
        return ''
    if word.isnumeric():
        return word
    if is_time(word) or is_special_pattern(word):
        return word
    if word.upper() in word_list:
        return word
    if word.upper() in proper_nouns:
        return word
    if word.upper() in interjections:
        return word
    if word in suggestions_made:
        return suggestions_made[word]
    # find index of current word in list of tokens
    for i, token in enumerate(tokens):
        if word in token:
            index = i
            break

    # Get 4 tokens before and after current word
    context_before = ' '.join(tokens[max(0,index-4):index])
    context_after = ' '.join(tokens[index+1:index+5])

    suggestion = ' '.join(segment(word))
    print(f"Context: {context_before} [{word}] {context_after}")
    print(f"Suggestion for ::: {word} -> {suggestion} :::")
    while True:
        choice = input("Enter 1 to accept change, 2 to reject change, or SPACE to enter custom text: ")
        if choice == '1':
            suggestions_made[word] = suggestion
            return suggestion
        elif choice == '2':
            suggestions_made[word] = word
            return word
        elif choice == ' ':
            custom_text = input("Enter custom text: ")
            suggestions_made[word] = custom_text
            return custom_text


filenames = glob.glob("books/*.txt")

for file in filenames:
    text = open(file, 'r').read().splitlines()
    with open(file, 'w') as f:
        f.write('')
    file = file.replace('books/', '')
    text = [line for line in text if line != '']
    for line in text:
        print(line)
        line = replace_periods_in_ellipsis(line)
        line = add_space_after_punctuation(line)
        line = line.replace("\xa0", " ")
        line = line.replace("\t", " ")
        # line = re.sub(r'(\w+)\d+', r'\1', line)
        # line = re.sub(r'(\w+)\d+', r'\1', line)
        # line = re.sub(r'(\w+)\d+', r'\1', line)
        # line = line.replace("_", "")
        line = line.replace("…", " ... ")
        line = line.replace(" . ", ". ")
        line = line.replace(" , ", ", ")
        line = line.replace('—', '-')
        line = line.replace('–', '-')
        line = line.replace('“', '"')
        line = line.replace('”', '"')
        line = line.replace('‘', "'")
        line = line.replace('’', "'")
        line = line.replace(". . .'", "... '")
        line = re.sub(r'(\w)([:;)])(\w)', r'\1\2 \3', line)
        line = re.sub(r'(\w)([.,?!])\'(\w)', r'\1\2 \'\3', line)
        line = line.replace("''", "' '")
        cleanline = line.replace(" .", ".")
        cleanline = cleanline.replace(" ,", ",")
        cleanline = cleanline.replace(" :", ":")
        cleanline = cleanline.replace(" ;", ";")
        cleanline = cleanline.replace(" !", "!")
        cleanline = cleanline.replace(" ?", "?")
        cleanline = cleanline.replace(" )", ")")
        cleanline = cleanline.replace("( ", "(")
        cleanline = cleanline.replace(" ]", "]")
        cleanline = cleanline.replace("[ ", "[")
        cleanline = cleanline.replace(" }", "}")
        cleanline = cleanline.replace("{ ", "{")
        cleanline = cleanline.replace('-', ' ')
        cleanline = cleanline.replace('—', ' ')

        cleanline = cleanline.split(' ')
        for unit in cleanline:
            # first scrub out the punctuation
            clean_unit = re.sub(r'^[^\w]+|[^\w]+$', '', unit)

            # then scrub away the pure numbers
            if clean_unit.isnumeric():
                continue

            clean_unit = clean_unit.split("'")
            if len(clean_unit) == 2 and clean_unit[1].lower() != "s":  # Check if it's not a possessive form
                print(f"file: {file}")
                print(f"line: {line}")
                print(f"Word: {clean_unit[0]}")
                print(f"Tokens: {cleanline}")
                
                index = line.index(clean_unit[0])
                new_word = process_word(clean_unit[0], cleanline)
                line = line[:index] + new_word + line[index+len(clean_unit[0]):]
                print(f"Word: {clean_unit[1]}")
                print(f"Tokens: {cleanline}")
                try:
                    index = line.index(clean_unit[1])
                except:
                    print(f"Error: {clean_unit[0]} not found in {line}")
                    print(f"Tokens: {cleanline}")
                    print(f"File: {file}")
                    with open('errors.txt', 'w') as f:
                        for key, value in suggestions_made.items():
                            f.write(f"{key}\t{value}\n")
                    exit()
                new_word = process_word(clean_unit[1], cleanline)
                line = line[:index] + new_word + line[index+len(clean_unit[1]):]
                continue
            else:
                print(f"Word: {clean_unit[0]}")
                print(f"Tokens: {cleanline}")
                try:
                    index = line.index(clean_unit[0])
                except:
                    print(f"Error: {clean_unit[0]} not found in {line}")
                    print(f"Tokens: {cleanline}")
                    print(f"File: {file}")
                    with open('errors.txt', 'w') as f:
                        for key, value in suggestions_made.items():
                            f.write(f"{key}\t{value}\n")
                    exit()
                new_word = process_word(clean_unit[0], cleanline)
                line = line[:index] + new_word + line[index+len(clean_unit[0]):]
                continue
     
        with open(f"books/{file}", 'a') as f:
            f.write(f"{line}\n")


with open('errors_segmenter.txt', 'w') as f:
    for key, value in suggestions_made.items():
        f.write(f"{key}\t{value}\n")