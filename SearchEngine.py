import sqlite3
from itertools import combinations
try:
    import spacy
    from flask import Flask
    from flask import render_template, send_file, request
except ImportError:
    # install parser and lemmatizer
    print("pip install spacy, Flask")
    print("python -m spacy download en_core_web_sm")

DATABASE = "DOCINDEX.db"

nlp = spacy.load("en_core_web_sm", disable=["ner", "parser"])
app = Flask(__name__,  static_folder="WEBPAGES_RAW", static_url_path='')

class SearchEngine:
    def __init__(self):
        self.db = sqlite3.connect(DATABASE, check_same_thread=False)
        self.cur = self.db.cursor()

    def __del__(self):
        self.cur.close()
        self.db.close()
        
    def get_lemmatized_words(self, txt: str) -> list:
        """return list of lemmatized words without stop-words"""
        doc = nlp(txt)
        return [token.lemma_ for token in doc if token.is_alpha and not token.is_stop]

    def simple_query(self, word: str):
        """simple query with one word"""

        word = self.get_lemmatized_words(word)[0]
        row = self.cur.execute(f'SELECT rowid FROM words WHERE word=?', (word, )).fetchone()
        wordid = ''
        if row:
            wordid = row[0]
        else:
            print('No such term in documents')
            return []
        rows = self.cur.execute("""
            SELECT documents.title, wordlocation.code, documents.url, wordlocation.tfidf, wordlocation.position, wordlocation.tag 
            FROM wordlocation, documents
            WHERE documents.code=wordlocation.code and wordid=?
            GROUP BY documents.code 
            ORDER by tfidf DESC, position ASC LIMIT 30""", (wordid, ))
        return rows.fetchall()

    def query(self, query: str):
        """main function for quering index database"""

        words = self.get_lemmatized_words(query.lower())
        if len(words)==1:
            return self.simple_query(words[0])
        wordids = []
        for word in words:
            row = self.cur.execute(f'SELECT rowid FROM words WHERE word=?', (word, )).fetchone()
            if row:
                wordids.append(row[0])
        docs = []
        # make list of documents contains one word from query
        for wordid in wordids:
            self.cur.execute(f'SELECT DISTINCT code, tfidf, position FROM wordlocation WHERE wordid=?', (wordid,))
            docs.append({row[0]: (row[1], row[2]) for row in self.cur.fetchall()})
        
        # distance between word position
        diffsum = lambda x: sum((j - i) for i, j in zip(x[:-1], x[1:]))
        # distance sorted so difference is positive
        # for tf-idf sum all values
        score_func = lambda x : (sum(el[0] for el in x), diffsum(sorted(el[1] for el in x)))
        # check all combinations of words from all terms
        # for example for 3-term query 
        # check 1,2,3 if found return best if not check [1,2], [1,3]...
        for terms_count in range(len(wordids), 0, -1):
            for c in combinations(range(len(wordids)), terms_count):
                common_docs = set.intersection(*map(set, [docs[k].keys() for k in c]))
                if not common_docs: continue
                res = {doc: score_func([docs[i][doc] for i in range(len(docs))]) for doc in common_docs}
                # normalize euclidean diference of position and tf-idf to 0,1 - range
                max_tf = res[max(res, key=lambda x: res[x][0])][0]
                min_dist = res[min(res, key=lambda x: res[x][1])][1]
                # calculate scores with weights 1, 1
                top_docs = sorted(res, key=lambda x: (res[x][0]/max_tf + min_dist/res[x][1]), reverse=True)[:min(len(res), 30)]
                rows = []
                for doc in top_docs:
                    self.cur.execute(f"SELECT title, code, url FROM documents WHERE code=?", (doc, ))
                    rows.append(self.cur.fetchone())
                return rows
        

@app.route('/', methods=['POST', 'GET'])
def simple_search():
    results = []
    if request.method == 'POST':
        results=[]
        query = request.form['search_input']
        if query:
            results = se.query(query)
    return render_template('index.html', results=results)

@app.route('/<path:path>')
def serve_page(path):
    return send_file('WEBPAGES_RAW', path,  mimetype="Content-Type: text/html; charset=utf-8")

if __name__ == "__main__":
    se = SearchEngine()
    app.run(debug=True)