from flask import render_template, Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)
 
@main.route("/<int:user_id>/ratings/<string:book_id>", methods=["GET"])
def book_ratings(user_id, book_id):
    logger.debug("User %s rating requested for book %s", user_id, book_id)
    hash_book_id = abs(hash(book_id)) % (10 ** 8)
    ratings = recommendation_engine.get_ratings_for_book_ids(user_id, [hash_book_id])
    return json.dumps(ratings)
 
 
@main.route("/<int:user_id>/ratings", methods=["POST"])
def add_ratings(user_id):
    ratings_data = request.get_json()  # 获取JSON数据
    ratings_list = ratings_data['ratings']
    ratings = [(user_id, int(rating['book_id']), float(rating['rating'])) for rating in ratings_list]
    recommendation_engine.add_ratings(ratings)

    return json.dumps(ratings_list)  # 返回接收到的数据以供确认

@main.route("/api/get_isbn_by_title", methods=["GET"])
def get_isbn_by_title():
    book_title = request.args.get('title', default='', type=str)
    if book_title:
        logger.info("API call to search ISBN for book title: %s", book_title)
        isbn_data = recommendation_engine.get_isbn_by_title(book_title)
        if isbn_data:
            return json.dumps(isbn_data)
        else:
            return json.dumps({'error': 'ISBN not found for the specified title'}), 404
    else:
        return json.dumps({'error': 'No title provided'}), 400

@main.route("/")
def index():
    # 渲染templates/index.html模板
    return render_template("index.html")

@main.route("/recommendation")
def recommendation():
    # 渲染templates/book_recommendation.html模板
    return render_template("book_recommendation.html")
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
