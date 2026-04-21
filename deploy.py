import boto3
import subprocess
import os
import json

class AgentCoreDeployer:
    def __init__(self, region='us-west-2'):
        self.region = region
        self.agentcore_client = boto3.client('bedrock-agentcore', region_name=region)
        self.ecr_client = boto3.client('ecr', region_name=region)
        
    def build_and_push_image(self, repository_name: str):
        """Build and push Docker image to ECR"""
        try:
            # Get ECR login token
            token_response = self.ecr_client.get_authorization_token()
            token = token_response['authorizationData'][0]['authorizationToken']
            endpoint = token_response['authorizationData'][0]['proxyEndpoint']
            
            # Docker login
            subprocess.run([
                'docker', 'login', '--username', 'AWS', 
                '--password-stdin', endpoint
            ], input=token, text=True, check=True)
            
            # Build image
            image_tag = f"{endpoint.replace('https://', '')}/{repository_name}:latest"
            subprocess.run([
                'docker', 'build', '-t', image_tag, '.'
            ], check=True)
            
            # Push image
            subprocess.run([
                'docker', 'push', image_tag
            ], check=True)
            
            print(f"Image pushed successfully: {image_tag}")
            return image_tag
            
        except Exception as e:
            print(f"Error building/pushing image: {e}")
            raise
    
    def create_runtime(self, name: str, image_uri: str):
        """Create AgentCore Runtime"""
        try:
            response = self.agentcore_client.create_runtime(
                runtimeName=name,
                description="Gemini Live Pro Speech-to-Speech Agent",
                runtimeConfig={
                    'image': {
                        'uri': image_uri
                    },
                    'resources': {
                        'memory': '2Gi',
                        'cpu': '1000m'
                    },
                    'networking': {
                        'protocols': ['websocket', 'http']
                    },
                    'runtime': {
                        'maxSessionDuration': 3600,
                        'idleTimeout': 300
                    }
                },
                environmentVariables={
                    'GEMINI_API_KEY': os.getenv('GEMINI_API_KEY')
                }
            )
            
            runtime_arn = response['runtimeArn']
            print(f"Runtime created successfully: {runtime_arn}")
            return runtime_arn
            
        except Exception as e:
            print(f"Error creating runtime: {e}")
            raise
    
    def deploy(self, runtime_name: str, repository_name: str):
        """Complete deployment process"""
        try:
            print("Starting deployment...")
            
            # Build and push image
            image_uri = self.build_and_push_image(repository_name)
            
            # Create runtime
            runtime_arn = self.create_runtime(runtime_name, image_uri)
            
            print(f"Deployment completed successfully!")
            print(f"Runtime ARN: {runtime_arn}")
            print(f"WebSocket endpoint: wss://bedrock-agentcore.{self.region}.amazonaws.com/runtimes/{runtime_arn}/ws")
            
            return runtime_arn
            
        except Exception as e:
            print(f"Deployment failed: {e}")
            raise

if __name__ == "__main__":
    deployer = AgentCoreDeployer()
    runtime_arn = deployer.deploy(
        runtime_name="gemini-voice-agent",
        repository_name="gemini-voice-agent"
    )
