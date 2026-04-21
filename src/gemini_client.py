import asyncio
import json
import logging
import os
import aiohttp
import websockets
from typing import Dict, Any, AsyncGenerator

logger = logging.getLogger(__name__)

class GeminiLiveClient:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        self.base_url = "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent"
        self.sessions = {}
    
    async def create_session(self) -> str:
        """Create a new Gemini Live Pro session"""
        try:
            # Connect to Gemini Live Pro WebSocket
            uri = f"{self.base_url}?key={self.api_key}"
            
            websocket = await websockets.connect(
                uri,
                extra_headers={
                    "Content-Type": "application/json"
                }
            )
            
            # Send setup message
            setup_message = {
                "setup": {
                    "model": "models/gemini-3.1-flash-live",
                    "generation_config": {
                        "response_modalities": ["AUDIO"],
                        "speech_config": {
                            "voice_config": {
                                "prebuilt_voice_config": {
                                    "voice_name": "Aoede"
                                }
                            }
                        }
                    },
                    "system_instruction": {
                        "parts": [{
                            "text": "You are a helpful voice assistant. Respond naturally and conversationally. Keep responses concise but informative."
                        }]
                    }
                }
            }
            
            await websocket.send(json.dumps(setup_message))
            
            # Wait for setup confirmation
            response = await websocket.recv()
            setup_response = json.loads(response)
            
            if "setupComplete" not in setup_response:
                raise Exception(f"Gemini setup failed: {setup_response}")
            
            session_id = f"gemini_session_{id(websocket)}"
            self.sessions[session_id] = {
                'websocket': websocket,
                'active': True
            }
            
            logger.info(f"Gemini Live Pro session created: {session_id}")
            return session_id
            
        except Exception as e:
            logger.error(f"Failed to create Gemini session: {e}")
            raise
    
    async def send_audio(self, session_id: str, audio_data: bytes):
        """Send audio data to Gemini Live Pro"""
        try:
            session = self.sessions.get(session_id)
            if not session or not session['active']:
                raise Exception(f"Invalid session: {session_id}")
            
            websocket = session['websocket']
            
            # Convert audio to base64 and send
            import base64
            audio_b64 = base64.b64encode(audio_data).decode()
            
            message = {
                "realtimeInput": {
                    "mediaChunks": [{
                        "mimeType": "audio/pcm",
                        "data": audio_b64
                    }]
                }
            }
            
            await websocket.send(json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error sending audio to Gemini: {e}")
            raise
    
    async def send_text(self, session_id: str, text: str):
        """Send text to Gemini Live Pro"""
        try:
            session = self.sessions.get(session_id)
            if not session or not session['active']:
                raise Exception(f"Invalid session: {session_id}")
            
            websocket = session['websocket']
            
            message = {
                "clientContent": {
                    "turns": [{
                        "role": "user",
                        "parts": [{
                            "text": text
                        }]
                    }],
                    "turnComplete": True
                }
            }
            
            await websocket.send(json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error sending text to Gemini: {e}")
            raise
    
    async def end_turn(self, session_id: str):
        """Signal end of turn to Gemini"""
        try:
            session = self.sessions.get(session_id)
            if not session or not session['active']:
                return
            
            websocket = session['websocket']
            
            message = {
                "clientContent": {
                    "turnComplete": True
                }
            }
            
            await websocket.send(json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error ending turn with Gemini: {e}")
    
    async def stream_responses(self, session_id: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream responses from Gemini Live Pro"""
        try:
            session = self.sessions.get(session_id)
            if not session or not session['active']:
                return
            
            websocket = session['websocket']
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    
                    # Handle server content (audio/text responses)
                    if "serverContent" in data:
                        server_content = data["serverContent"]
                        
                        if "modelTurn" in server_content:
                            model_turn = server_content["modelTurn"]
                            
                            for part in model_turn.get("parts", []):
                                if "inlineData" in part:
                                    # Audio response
                                    audio_data = part["inlineData"]["data"]
                                    import base64
                                    audio_bytes = base64.b64decode(audio_data)
                                    
                                    yield {
                                        "type": "audio",
                                        "audio": audio_bytes
                                    }
                                
                                elif "text" in part:
                                    # Text response
                                    yield {
                                        "type": "text",
                                        "text": part["text"]
                                    }
                        
                        if server_content.get("turnComplete"):
                            yield {
                                "type": "turn_complete"
                            }
                    
                    # Handle tool calls or other server messages
                    elif "toolCall" in data:
                        # Handle tool calls if needed
                        pass
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse Gemini response: {e}")
                except Exception as e:
                    logger.error(f"Error processing Gemini response: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Gemini session {session_id} disconnected")
        except Exception as e:
            logger.error(f"Error streaming from Gemini session {session_id}: {e}")
    
    async def close_session(self, session_id: str):
        """Close Gemini Live Pro session"""
        try:
            session = self.sessions.pop(session_id, None)
            if session:
                session['active'] = False
                websocket = session['websocket']
                await websocket.close()
                logger.info(f"Gemini session closed: {session_id}")
        except Exception as e:
            logger.error(f"Error closing Gemini session {session_id}: {e}")
