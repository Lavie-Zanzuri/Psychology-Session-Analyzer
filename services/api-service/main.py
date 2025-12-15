from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson import ObjectId
import os
from typing import List, Dict, Any
from datetime import datetime

app = FastAPI(title="Psychology Session Analyzer API")

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = "psychology_analyzer"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
analyses_collection = db["analyses"]

def serialize_mongo_doc(doc):
    """Convert MongoDB document to JSON-serializable dict"""
    if doc is None:
        return None
    doc["id"] = str(doc.pop("_id"))
    if "created_at" in doc and isinstance(doc["created_at"], datetime):
        doc["created_at"] = doc["created_at"].isoformat()
    return doc

@app.get("/")
def root():
    """Root endpoint"""
    return {
        "message": "Psychology Session Analyzer API",
        "version": "1.0.0",
        "endpoints": {
            "videos": "/videos",
            "analysis": "/videos/{id}/analysis"
        }
    }

@app.get("/videos")
def get_videos():
    """Get list of all analyzed videos"""
    try:
        # Get all analyses, but only return basic info
        videos = []
        for doc in analyses_collection.find().sort("created_at", -1):
            videos.append({
                "id": str(doc["_id"]),
                "video_filename": doc.get("video_filename"),
                "audio_filename": doc.get("audio_filename"),
                "transcription_filename": doc.get("transcription_filename"),
                "created_at": doc.get("created_at").isoformat() if doc.get("created_at") else None,
                "total_utterances": doc.get("analysis", {}).get("summary", {}).get("total_utterances", 0),
                "patient_utterances": doc.get("analysis", {}).get("summary", {}).get("patient_utterances", 0)
            })
        
        return {
            "total": len(videos),
            "videos": videos
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching videos: {str(e)}")

@app.get("/videos/{video_id}/analysis")
def get_video_analysis(video_id: str):
    """Get full analysis of a specific video"""
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(video_id):
            raise HTTPException(status_code=400, detail="Invalid video ID format")
        
        # Find the analysis
        doc = analyses_collection.find_one({"_id": ObjectId(video_id)})
        
        if not doc:
            raise HTTPException(status_code=404, detail="Video not found")
        
        # Return full document
        return serialize_mongo_doc(doc)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching analysis: {str(e)}")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        # Check MongoDB connection
        mongo_client.admin.command('ping')
        return {
            "status": "healthy",
            "mongodb": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "mongodb": "disconnected",
            "error": str(e)
        }