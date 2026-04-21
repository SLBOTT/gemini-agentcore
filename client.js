// client.js - Example WebSocket client
class GeminiVoiceClient {
    constructor(runtimeArn, region = 'us-east-1') {
        this.runtimeArn = runtimeArn;
        this.region = region;
        this.websocket = null;
        this.mediaRecorder = null;
        this.audioContext = null;
    }
    
    async connect(bearerToken) {
        try {
            const wsUrl = `wss://bedrock-agentcore.${this.region}.amazonaws.com/runtimes/${this.runtimeArn}/ws`;
            
            this.websocket = new WebSocket(wsUrl, [], {
                headers: {
                    'Authorization': `Bearer ${bearerToken}`
                }
            });
            
            this.websocket.onopen = () => {
                console.log('Connected to Gemini Voice Agent');
            };
            
            this.websocket.onmessage = (event) => {
                this.handleMessage(JSON.parse(event.data));
            };
            
            this.websocket.onclose = () => {
                console.log('Disconnected from agent');
            };
            
            this.websocket.onerror = (error) => {
                console.error('WebSocket error:', error);
            };
            
        } catch (error) {
            console.error('Connection failed:', error);
        }
    }
    
    async startRecording() {
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ 
                audio: {
                    sampleRate: 24000,
                    channelCount: 1,
                    echoCancellation: true,
                    noiseSuppression: true
                } 
            });
            
            this.mediaRecorder = new MediaRecorder(stream, {
                mimeType: 'audio/webm;codecs=pcm'
            });
            
            this.mediaRecorder.ondataavailable = (event) => {
                if (event.data.size > 0) {
                    this.sendAudio(event.data);
                }
            };
            
            this.mediaRecorder.start(100); // Send chunks every 100ms
            console.log('Recording started');
            
        } catch (error) {
            console.error('Failed to start recording:', error);
        }
    }
    
    stopRecording() {
        if (this.mediaRecorder) {
            this.mediaRecorder.stop();
            this.mediaRecorder = null;
            console.log('Recording stopped');
        }
    }
    
    async sendAudio(audioBlob) {
        try {
            const arrayBuffer = await audioBlob.arrayBuffer();
            const base64Audio = btoa(String.fromCharCode(...new Uint8Array(arrayBuffer)));
            
            this.websocket.send(JSON.stringify({
                type: 'audio',
                audio: base64Audio,
                format: 'webm',
                sample_rate: 24000
            }));
            
        } catch (error) {
            console.error('Failed to send audio:', error);
        }
    }
    
    sendText(text) {
        this.websocket.send(JSON.stringify({
            type: 'text',
            text: text
        }));
    }
    
    endTurn() {
        this.websocket.send(JSON.stringify({
            type: 'control',
            action: 'end_turn'
        }));
    }
    
    handleMessage(message) {
        switch (message.type) {
            case 'audio_response':
                this.playAudio(message.audio);
                break;
            case 'text_response':
                console.log('Agent response:', message.text);
                break;
            case 'session_started':
                console.log('Session started:', message.session_id);
                break;
            case 'turn_complete':
                console.log('Turn completed');
                break;
            case 'error':
                console.error('Agent error:', message.message);
                break;
        }
    }
    
    async playAudio(base64Audio) {
        try {
            const audioData = atob(base64Audio);
            const audioBuffer = new ArrayBuffer(audioData.length);
            const view = new Uint8Array(audioBuffer);
            
            for (let i = 0; i < audioData.length; i++) {
                view[i] = audioData.charCodeAt(i);
            }
            
            if (!this.audioContext) {
                this.audioContext = new AudioContext();
            }
            
            const decodedAudio = await this.audioContext.decodeAudioData(audioBuffer);
            const source = this.audioContext.createBufferSource();
            source.buffer = decodedAudio;
            source.connect(this.audioContext.destination);
            source.start();
            
        } catch (error) {
            console.error('Failed to play audio:', error);
        }
    }
    
    disconnect() {
        if (this.websocket) {
            this.websocket.close();
        }
        this.stopRecording();
    }
}

// Usage example
const client = new GeminiVoiceClient('your-runtime-arn');
client.connect('your-bearer-token');
