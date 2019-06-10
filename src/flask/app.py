from flask import Flask, flash, redirect, render_template, request, session, abort, send_from_directory
from random import randint
from cassandra.cluster import Cluster
from entity_codes import category_names
from configs import cassandra_cluster_ips
import os

def get_category_name(code):
   if code in category_names:
        return category_names[code]
   return code

cluster = Cluster(cassandra_cluster_ips)
session = cluster.connect('hawkeye')

app = Flask(__name__)

@app.route("/")
def index():
    response = render_template('home.html',**locals())
    return response

# @app.route('/build/img/<path:filename>')
# def serve_static(filename):
#     root_dir = os.path.dirname(os.getcwd())
#     return send_from_directory(os.path.join(root_dir, 'static', 'build', 'img'), filename)

@app.route("/yearly")
def yearly():
    country = request.args.get('country')
    country_code = request.args.get('code').upper()
    year = request.args.get('year')

    cql = "SELECT * FROM yearly WHERE country = %s and date = %s"
    rs = session.execute(cql, parameters=[country_code, int(year)])[0]
    scores = rs[2]
    scoreMap = {}
    for cat in scores:
        scoreMap[get_category_name(str(cat))] = {str(key): value for key, value in scores[cat].items()}

    top_articles = rs[3]
    topMap = {}
    def getArticle(article):
        articleMap = {str(key): value for key,value in article.items()}
        return articleMap
    for cat in top_articles:
        topMap[get_category_name(str(cat))] = [getArticle(article) for article in top_articles[cat]]

    cqlPerMonth = "SELECT * FROM monthly WHERE country = %s and date >= %s01 and date <= %s12"
    rsPerMonth = session.execute(cqlPerMonth, parameters=[country_code, int(year), int(year)])
    monthlyScores = []
    for rsMonth in rsPerMonth:
        scores = rsMonth[2]
        scoreMap = {}
        for cat in scores:
            scoreMap[get_category_name(str(cat))] = {str(key): value for key, value in scores[cat].items()}
        monthlyScores.append(scoreMap)

    return render_template("yearly.html", **locals())

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
