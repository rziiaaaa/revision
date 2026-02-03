from flask import Blueprint, jsonify, render_template
from . import mysql
from .controller.BookController import list_books, ca_by_publisher, variation_2015_2016

app = Blueprint("api", __name__)

@app.route("/")
def docs():
    return render_template("doc.html")

@app.route("/books", endpoint="getBooks")
def get_books():
    return jsonify(list_books(mysql))

@app.route("/publishers/ca", endpoint="getCAByPublisher")
def get_ca_by_publisher():
    return jsonify(ca_by_publisher(mysql))

@app.route("/sales/variation", endpoint="getVariation")
def get_variation():
    return jsonify(variation_2015_2016(mysql))
