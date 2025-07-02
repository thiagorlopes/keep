import os
import logging
from flask import Flask
from .api.routes import api_bp

def create_app():
    """Create and configure an instance of the Flask application."""
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.config['API_MOCK_SECRET_KEY'] = os.getenv("API_MOCK_SECRET_KEY")

    # Register the blueprint
    app.register_blueprint(api_bp)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    app.logger.info("Flask app created and configured.")

    return app
