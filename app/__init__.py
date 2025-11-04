"""
Sample Log microservice - Python

"""

__version__ = "1.0.0"
__author__ = "Medical App Team"
__description__ = "User management microservice with FastAPI and PostgreSQL"

# Import main components for easier access
from app.main import app
from app.config import config
from app.database import db
from app.log_dto import log_dto
from app.schemas import schemas

from app.crud import crud_service

# Optional: Initialize package-level variables
package_initialized = False

def initialize_package():
    """Initialize package components"""
    global package_initialized