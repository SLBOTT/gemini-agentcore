import asyncio
import json
import logging
import os
from typing import Dict, Any
import websockets
from websockets.server import serve
import base64
import io

from gemini_client import GeminiLiveClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GeminiVoiceAgent:
    def __init__(self):
        self.gemini_client = GeminiLiveClient()
        self.active_sessions = {}
        
    async def start_server(self):
        """Start the WebSocket server on port 8080 at /ws path"""
        logger.info("Starting Gemini Voice Agent WebSocket server...")
        
        # Health check endpoint
        async def health_check(websocket, path):
            if path == "/ping":
                await websocket.send("pong")
                await websocket.close()
            else:
                await websocket.close(code=1000, reason="Invalid path")
        
        # Main WebSocket handler
        async def handle_websocket(websocket, path):
            if path == "/ws":
                await self.handle_voice_session(websocket)
            elif path == "/ping":
                await websocket.send("pong")
                await websocket.close()
            else:
                await websocket.close(code=1000, reason="Invalid path")
        
        # Start server on port 8080 (required by AgentCore)
        server = await serve(
            handle_websocket,
            "0.0.0.0",  # Required by AgentCore
            8080,       # Required by AgentCore
            ping_interval=30,
            ping_timeout=10
        )
        
        logger.info("WebSocket server started on ws://0.0.0.0:8080/ws")
        await server.wait_closed()
    
    async def handle_voice_session(self, websocket):
        """Handle individual voice session with bidirectional streaming"""
        session_id = id(websocket)
        logger.info(f"New voice session started: {session_id}")
        
        try:
            # Initialize Gemini Live session
            gemini_session = await self.gemini_client.create_session()
            self.active_sessions[session_id] = {
                'websocket': websocket,
                'gemini_session': gemini_session,
                'audio_buffer': io.BytesIO()
            }
            
            # Send initial greeting
            await websocket.send(json.dumps({
                "type": "session_started",
                "session_id": str(session_id),
                "message": "Voice agent ready. Start speaking!"
            }))
            
            # Handle bidirectional streaming
            await asyncio.gather(
                self.handle_client_audio(websocket, session_id),
                self.handle_gemini_responses(websocket, session_id)
            )
            
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Session {session_id} disconnected")
        except Exception as e:
            logger.error(f"Error in session {session_id}: {e}")
            await websocket.send(json.dumps({
                "type": "error",
                "message": f"Session error: {str(e)}"
            }))
        finally:
            # Cleanup session
            if session_id in self.active_sessions:
                await self.cleanup_session(session_id)
    
    async def handle_client_audio(self, websocket, session_id):
        """Handle incoming audio from client"""
        try:
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "audio":
                    # Decode base64 audio data
                    audio_data = base64.b64decode(data["audio"])
                    
                    # Send to Gemini Live Pro
                    await self.send_audio_to_gemini(session_id, audio_data)
                    
                elif data.get("type") == "text":
                    # Handle text input
                    await self.send_text_to_gemini(session_id, data["text"])
                    
                elif data.get("type") == "end_turn":
                    # Signal end of user turn
                    await self.end_turn_gemini(session_id)
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client disconnected from session {session_id}")
        except Exception as e:
            logger.error(f"Error handling client audio for session {session_id}: {e}")
    
    async def handle_gemini_responses(self, websocket, session_id):
        """Handle responses from Gemini Live Pro"""
        try:
            session = self.active_sessions[session_id]
            gemini_session = session['gemini_session']
            
            async for response in self.gemini_client.stream_responses(gemini_session):
                if response.get("type") == "audio":
                    # Send audio response to client
                    await websocket.send(json.dumps({
                        "type": "audio_response",
                        "audio": base64.b64encode(response["audio"]).decode(),
                        "format": "pcm16",
                        "sample_rate": 24000
                    }))
                    
                elif response.get("type") == "text":
                    # Send text response to client
                    await websocket.send(json.dumps({
                        "type": "text_response",
                        "text": response["text"]
                    }))
                    
                elif response.get("type") == "turn_complete":
                    # Signal turn completion
                    await websocket.send(json.dumps({
                        "type": "turn_complete"
                    }))
                    
        except Exception as e:
            logger.error(f"Error handling Gemini responses for session {session_id}: {e}")
    
    async def send_audio_to_gemini(self, session_id: str, audio_data: bytes):
        """Send audio data to Gemini Live Pro"""
        try:
            session = self.active_sessions[session_id]
            gemini_session = session['gemini_session']
            
            await self.gemini_client.send_audio(gemini_session, audio_data)
            
        except Exception as e:
            logger.error(f"Error sending audio to Gemini: {e}")
    
    async def send_text_to_gemini(self, session_id: str, text: str):
        """Send text to Gemini Live Pro"""
        try:
            session = self.active_sessions[session_id]
            gemini_session = session['gemini_session']
            
            await self.gemini_client.send_text(gemini_session, text)
            
        except Exception as e:
            logger.error(f"Error sending text to Gemini: {e}")
    
    async def end_turn_gemini(self, session_id: str):
        """Signal end of turn to Gemini"""
        try:
            session = self.active_sessions[session_id]
            gemini_session = session['gemini_session']
            
            await self.gemini_client.end_turn(gemini_session)
            
        except Exception as e:
            logger.error(f"Error ending turn with Gemini: {e}")
    
    async def cleanup_session(self, session_id: str):
        """Clean up session resources"""
        try:
            session = self.active_sessions.pop(session_id, None)
            if session:
                gemini_session = session['gemini_session']
                await self.gemini_client.close_session(gemini_session)
                logger.info(f"Session {session_id} cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up session {session_id}: {e}")

# Main entry point
async def main():
    agent = GeminiVoiceAgent()
    await agent.start_server()

if __name__ == "__main__":
    asyncio.run(main())
