#!/usr/bin/env python3

import json
import math
import sqlite3
from pathlib import Path
from sqlite3.dbapi2 import Connection
from collections import Counter

try:
    import spacy
    from bs4 import BeautifulSoup
except ImportError:
    # install parser and lemmatizer
    print("pip install spacy, bs4")
    print("python -m spacy download en_core_web_sm")

DATABASE = "DOCINDEX.db"
# load lemmatizer
nlp = spacy.load("en_core_web_sm", disable=["ner", "parser"])


class IndexCreator:
    def __init__(self):
        self.db = sqlite3.connect(DATABASE)
        self.cur = self.db.cursor()
        # documents - table for document info - title, url, wordcount
        # code field - document code from json file (0/1, 11/21....)
        # words - table for word, other tables ref to pk wordid (rowid in sqlite)
        # doctf - aux table for TF calculation for given document and word
        # worddoccnt - aux table for IDF calculation for given word
        # all calculation may be done without aux tables direct from wordlocation table
        # with advanced and nested sql queries
        # wordlocation - main table (INVERSE INDEX with tf-idf and metadata position and tagname)
        self.cur.executescript(
            r"""CREATE table IF NOT EXISTS documents(code TEXT, title TEXT, url TEXT, wordcount INTEGER);
            CREATE table IF NOT EXISTS words(word TEXT);
            CREATE table IF NOT EXISTS doctf(code TEXT, wordid INTEGER, tf REAL);
            CREATE table IF NOT EXISTS worddoccnt(wordid INTEGER, doccount INTEGER, idf REAL);
            CREATE table IF NOT EXISTS wordlocation(wordid INTEGER, code TEXT, position INTEGER, 
            tag TEXT, tfidf REAL);
           CREATE INDEX IF NOT EXISTS documents_idx ON documents(CODE);
           CREATE INDEX IF NOT EXISTS words_idx ON words(word);
           CREATE INDEX IF NOT EXISTS doctf_idx ON doctf(CODE, wordid);
           CREATE INDEX IF NOT EXISTS worddoccnt_idx ON worddoccnt(wordid);
           CREATE INDEX IF NOT EXISTS wordlocation_idx ON wordlocation(wordid, code);
           """)
        self.db.commit()

    def _process_files(self):
        """process all files"""

        with open("WEBPAGES_RAW/bookkeeping.json", encoding="UTF-8") as json_file:
            data = json.load(json_file)
        for ind, (file, url) in enumerate(data.items()):
            try:
                # if document in database skip, so we can break calculation by 
                # Ctrl+C see KeyboardInterrupt and continue parse new files
                self.cur.execute(f'SELECT * FROM documents WHERE code=?', (file, ))
                if self.cur.fetchone():
                    continue
                self._process_html(f"WEBPAGES_RAW/{file}", file, url)
                print(ind, file)
            except Exception as e:
                print(e)
                continue
            except KeyboardInterrupt:
                break
        # update table with tf_idf 
        self._update_tf_idf()

    def get_lemmatized_words(self, txt: str) -> list:
        """return list of lemmatized words without stop-words"""
        doc = nlp(txt)
        return [token.lemma_ for token in doc if token.is_alpha and not token.is_stop]

    def _get_or_create_entry_id(self, table, field, entry, insert=True):
        """Get rowid from table if entry exist or create entry and return rowid"""

        self.cur.execute(f'SELECT rowid FROM {table} WHERE {field}=?', (entry, ))
        res = self.cur.fetchone()
        if res:
            return res[0]
        else:
            if insert:
                self.cur.execute(f'INSERT INTO {table} values (?)', (entry,))
                return self.cur.lastrowid

    def _process_html(self, html_path, file, url):
        """ parse single html file """ 

        def get_words_in_tag(tagname: str, soup) -> list:
            """collect lemmatized text from tag and remove tag so word dont count twice"""
            words = []
            for tag in soup.find_all(tagname):
                words.append(tag.text.lower().strip())
                tag.extract()
            return self.get_lemmatized_words(''.join(words))

        with open(html_path, encoding="utf8") as fp:
            soup = BeautifulSoup(fp, "lxml")
        if soup.find('title'):
            title_raw = soup.find('title').text.lower().strip()
        else:
            title_raw = ''
        text = []
        # collect text from html tags with corresponding tagname
        for tag in ('title', 'h1', 'h2', 'h3', 'b'):
            text.extend([(word, tag) for word in get_words_in_tag(tag, soup)])
        # get body text
        text.extend([(word, '') for word in self.get_lemmatized_words(
            soup.get_text(strip=True).lower())])
        # all lemmatized text from document with tagname and position in document
        all_words = [(ind, word, tag) for ind, (word, tag) in enumerate(text)]
        self.cur.execute('INSERT INTO documents VALUES (?,?,?,?)',
                         (file, title_raw, url, len(all_words)))
        # word_cnt for count word frequency in this document
        word_cnt = Counter()
        for (ind, word, tag) in all_words:
            wordid = self._get_or_create_entry_id('words', 'word', word)
            self.cur.execute('INSERT INTO wordlocation VALUES (?,?,?,?,?)',
                             (wordid, file, ind, tag, 0.0))
            word_cnt[word] += 1
        # insert data for aux table for tf-idf calculation
        for word, freq in word_cnt.items():
            wordid = self._get_or_create_entry_id('words', 'word', word)
            self.cur.execute('INSERT INTO doctf VALUES (?,?,?)', (file, wordid, freq/len(all_words)))
            res = self.cur.execute('SELECT doccount FROM worddoccnt where wordid=?', (wordid, ))
            if res.fetchone():
                self.cur.execute('UPDATE worddoccnt SET doccount=doccount+1 where wordid=?', 
                (wordid, ))
            else:
                self.cur.execute('INSERT INTO worddoccnt VALUES (?,?,?)', (wordid, 1, 0.0))
        self.db.commit()

    def _update_tf_idf(self):
        # all documents count
        all_docs = self.cur.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
        # create custom log10 function for sqlite
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function
        self.db.create_function('LOG10', 1, math.log10)
        # calculate IDF
        self.cur.execute('UPDATE worddoccnt SET idf=LOG10(?/doccount)', (float(all_docs), ))
        # calculate TF-IDF
        self.cur.execute("""
        UPDATE wordlocation 
        set tfidf=(SELECT doctf.tf from doctf where doctf.code=wordlocation.code and 
        doctf.wordid=wordlocation.wordid)*
        (SELECT worddoccnt.idf from worddoccnt where worddoccnt.wordid=wordlocation.wordid)
        """)
        self.db.commit()

    def __del__(self):
        self.cur.close()
        self.db.close()

if __name__ == "__main__":
    #create index
    ind = IndexCreator()
    #ind._process_files()
