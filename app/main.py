
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware 
from typing import List, Dict, Any
from pydantic import BaseModel
from app import schemas
from app.crud import crud_service
from app.database import db
import jwt


app = FastAPI(
    title="sample Log Microservice Api",
    description="API from Log microservice",
    version="1.0.1"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[""],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)