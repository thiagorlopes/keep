from flask import Flask

def create_app():
    """Create and configure an instance of the Flask application."""
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'dev' # Change for production

    from app.api.routes import api_bp
    app.register_blueprint(api_bp)

    return app
