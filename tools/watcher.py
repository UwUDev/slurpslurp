import asyncio
import json
import os
from pathlib import Path
from typing import List, Optional
from collections import deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import time
import urllib.parse

app = FastAPI()

@app.middleware("http")
async def decode_url_middleware(request, call_next):
    if request.url.path.startswith("/static/"):
        decoded_path = urllib.parse.unquote(request.url.path)
        request.scope["path"] = decoded_path
        request.scope["raw_path"] = decoded_path.encode()
    response = await call_next(request)
    return response

app.mount("/static", StaticFiles(directory="../downloads/image", html=True), name="static")

class ImageQueue:
    def __init__(self, max_size=50):
        self.queue = deque(maxlen=max_size)
        self.known_images = set()

    def add_image(self, image_path: str, timestamp: float):
        if image_path not in self.known_images:
            self.queue.append(
                {
                    "path": image_path,
                    "timestamp": timestamp,
                    "filename": os.path.basename(image_path),
                }
            )
            self.known_images.add(image_path)
            return True
        return False

    def get_next(self):
        if self.queue:
            return self.queue.popleft()
        return None

    def size(self):
        return len(self.queue)

    def clear_old_from_known(self):
        current_paths = {item["path"] for item in self.queue}
        self.known_images = current_paths

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.image_queue = ImageQueue()
        self.latest_image: Optional[dict] = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"New connection. Total: {len(self.active_connections)}")
        
        if self.latest_image:
            
            self.latest_image["path"] = self.latest_image["path"].replace(
                "../downloads/image/", ""
            )
            await self.send_to_client(websocket, self.latest_image, "current_image")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"Connection closed. Total: {len(self.active_connections)}")

    async def send_to_client(self, websocket: WebSocket, image_data: dict, msg_type: str):
        normalized_path = image_data["path"].replace("\\", "/")
        if normalized_path.startswith("./"):
            normalized_path = normalized_path[2:]
        message = json.dumps(
            {
                "type": msg_type,
                "path": f"/static/{normalized_path}",
                "timestamp": image_data["timestamp"],
                "filename": image_data["filename"],
                #"queue_size": self.image_queue.size(),
            }
        )
        try:
            await websocket.send_text(message)
        except:
            pass

    async def broadcast_image(self, image_data: dict):
        self.latest_image = image_data
        print(
            f"Broadcasting image: {image_data['filename']} to {len(self.active_connections)} clients"
        )
        
        disconnected = []
        for connection in self.active_connections:
            try:
                await self.send_to_client(connection, image_data, "new_image")
            except:
                disconnected.append(connection)
        
        for conn in disconnected:
            self.disconnect(conn)

manager = ConnectionManager()

def scan_images_directory(directory="../downloads/image"):
    images = []
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist")
        return images
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_time = os.path.getmtime(file_path)
                images.append(
                    {"path": file_path, "timestamp": file_time, "filename": file}
                )
            except OSError:
                continue
    
    images.sort(key=lambda x: x["timestamp"], reverse=True)
    return images

async def polling_task():
    last_scan_time = 0
    while True:
        try:
            current_time = time.time()
            print(
                f"Scanning image directory... Current queue: {manager.image_queue.size()}"
            )
            images = scan_images_directory()
            new_images_count = 0
            
            for image_data in images:
                if image_data["timestamp"] > last_scan_time:
                    if manager.image_queue.add_image(
                        image_data["path"], image_data["timestamp"]
                    ):
                        new_images_count += 1
            if new_images_count > 0:
                print(f"Added {new_images_count} new images to the queue")
            
            manager.image_queue.clear_old_from_known()
            last_scan_time = current_time
        except Exception as e:
            print(f"Error during polling: {e}")
        await asyncio.sleep(0.5)  

async def queue_processor():
    while True:
        try:
            next_image = manager.image_queue.get_next()
            
            if next_image and manager.active_connections:
                next_image["path"] = next_image["path"].replace(
                                    "../downloads/image/", ""
                                )
                await manager.broadcast_image(next_image)
                print(
                    f"Image sent: {next_image['filename']}, remaining queue: {manager.image_queue.size()}"
                )
        except Exception as e:
            print(f"Error during queue processing: {e}")
        await asyncio.sleep(0.1)  

@app.on_event("startup")
async def startup_event():
    print("Starting image monitoring system...")
    
    images = scan_images_directory()
    if images:
        latest = images[0]  
        manager.latest_image = latest
        print(f"Initial image found: {latest['filename']}")
    
    asyncio.create_task(polling_task())
    asyncio.create_task(queue_processor())
    print("Monitoring started on ./download/images")

@app.get("/")
async def get():
    return HTMLResponse(content=open("watcher.html").read(), media_type="text/html")

@app.get("/watcher.js")
async def get_js():
    return HTMLResponse(content=open("watcher.js").read(), media_type="text/javascript")

@app.get("/watcher.css")
async def get_css():
    return HTMLResponse(content=open("watcher.css").read(), media_type="text/css")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8882)
