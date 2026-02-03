from flask import Flask
from flask_cors import CORS
from flask_mysqldb import MySQL
import os

mysql = MySQL()

def create_app():
    app = Flask(__name__, template_folder="templates")
    CORS(app)

    app.config.update(
        MYSQL_HOST=os.getenv("MYSQL_HOST", "db"),
        MYSQL_USER=os.getenv("MYSQL_USER", "root"),
        MYSQL_PASSWORD=os.getenv("MYSQL_PASSWORD", "root"),
        MYSQL_DB=os.getenv("MYSQL_DB", "books"),
        MYSQL_PORT=int(os.getenv("MYSQL_PORT", "3306")),
        MYSQL_CURSORCLASS="DictCursor",
    )

    mysql.init_app(app)

    # enregistrer les routes
    from .routes import app as app_routes
    app.register_blueprint(app_routes)

    return app

# exposer une instance directe (utile pour FLASK_APP="app:app")
app = create_app()


