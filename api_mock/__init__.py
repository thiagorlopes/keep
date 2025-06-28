import logging
from flask import Flask
from .api.routes import api_bp

def create_app():
    """Create and configure an instance of the Flask application."""
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.config['SECRET_KEY'] = 'a_very_secret_key_that_should_be_changed'

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
