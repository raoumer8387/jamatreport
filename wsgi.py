import os
import sys

# Add your app directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import the Flask application from app.py
from app import application
