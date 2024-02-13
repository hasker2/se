from flask import Flask, render_template, request
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import aiosqlite
import asyncio
from urllib.parse import urlparse
from fuzzywuzzy import fuzz
import json
import time
import nltk
print("aaa")
app = Flask(__name__)
import nltk
nltk.download("stopwords")
stop_words = set(stopwords.words('english'))

MAX_FROM_ONE_NETLOC = 3

@app.route('/')
def index():
    return render_template('index.html')

async def execute_query(query, args=None):
    async with aiosqlite.connect("database.db", timeout=300) as db:
        async with db.execute(query, args) as cursor:
            return await cursor.fetchall()

async def add_cached_words(cw: list):
    async with aiosqlite.connect("database.db", timeout=300) as db:
        k = 1
        print(len(cw))
        full_query = ""
        for i in cw:
            if k % 100 == 0:
                print(k)
            try:
                first_key = list(i.keys())[0]

                words = await execute_query("select words from pages where hash = ?", (first_key,))
                if words[0][0] is not None:
                    obj = json.loads(words[0][0])
                    obj[first_key[0]] = list(i[first_key])
                    full_query += f"update pages set words = '{dump}' where hash = '{first_key}';\n"
                else:
                    dump = json.dumps({first_key: list(i[first_key])})
                    full_query += f"update pages set words = '{dump}' where hash = '{first_key}';\n"

            except Exception as e:
                pass
            k+=1
        print(full_query)
        await db.executescript(full_query)
        await db.commit()

cached_queries = {}

@app.route('/search')
async def query():
    start_time = time.time()
    ranked_queries = {}

    parsed_links = {}

    user_query = request.args.get("query")
    print(user_query)
    filtered_query = [i.lower() for i in user_query.split() if i not in stop_words]

    if tuple(sorted(filtered_query)) in cached_queries.keys():
        return render_template('query.html', results=cached_queries[tuple(sorted(filtered_query))], search_text=user_query, number=len(cached_queries[tuple(sorted(filtered_query))]),
                        time=round(time.time() - start_time, 2))

    pages_query = "SELECT content, hash, header, link FROM pages"
    pages = await execute_query(pages_query)

    used_headers = []

    cached_words = []

    for content, hash_value, header, link in pages:

        cached_words_hash = {hash_value: set()}
        #limiting results from the same url
        if urlparse(link).netloc not in parsed_links.keys():
            parsed_links[urlparse(link).netloc] = 1
        else:
            if parsed_links[urlparse(link).netloc] >= MAX_FROM_ONE_NETLOC:
                continue
            else:
                parsed_links[urlparse(link).netloc]+=1
        if header not in used_headers:
            used_headers.append(header)
        else:
            continue
        #avoiding two results with the same title


        rank = 0
        # checking if there are words from the query in the header
        if user_query in header:
            rank+=50
        else:
            for word in header.split():
                if word.lower() in filtered_query:
                    rank += 50/len(filtered_query)
                else:
                    for i in filtered_query:
                        if fuzz.ratio(i, word) > 70:
                            rank+=25/len(filtered_query)


        # checking if there are words from the query in the page content
        content_split = set(content.split()) # set to prevent from giving high rank because of multiple same words
        for word in content_split:
            if word.lower() in filtered_query:
                rank += 1
            else:
                for i in filtered_query:
                    if fuzz.ratio(i, word) > 70:
                        rank+=0.5


        # removing result if there is no keywords on it
        if rank == 0:
            cached_words_hash[hash_value].update(filtered_query)

        if len(cached_words_hash[hash_value]) > 0:

            cached_words.append(cached_words_hash)
        if rank == 0:
            continue
        ranked_queries[hash_value] = rank



    sorted_results = list(sorted(ranked_queries.items(), key=lambda item: item[1], reverse=True))

    results = []

    for hash_value, rank in sorted_results[:15]:
        title_link_query = "SELECT header, link, content FROM pages WHERE hash = ?"
        title_link = await execute_query(title_link_query, (hash_value,))

        desc_query = "SELECT SUBSTR(content, 1, 100) FROM pages WHERE hash = ?"
        desc = await execute_query(desc_query, (hash_value,))

        results.append({
            "title": title_link[0][0][:150],
            "link": title_link[0][1],
            "desc": str(desc[0][0])
        })
    #await add_cached_words(cached_words)
    cached_queries[tuple(sorted(filtered_query))] = results
    return render_template('query.html', results=results, search_text=user_query, number=len(sorted_results), time=round(time.time()-start_time, 2))

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
