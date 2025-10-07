#!/usr/bin/env python3
"""
Startup script for the Databricks MCP application
Handles both MCP server startup and Chainlit interface
"""

import subprocess
import sys
import time
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  

def check_requirements():
    """Check if all required packages are installed"""
    try:
        import chainlit
        import openai
        import mcp
        import databricks.sql
        import fastmcp
        print("✅ All required packages are installed")
        return True
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("Please run: pip install -r requirements.txt")
        return False

def check_env_file():
    """Check if .env file exists and has required variables"""
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ .env file not found")
        return False
    
    required_vars = [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HTTP_PATH",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_MODEL",
        "AZURE_OPENAI_API_VERSION",
    ]
    
    with open(env_file, 'r') as f:
        content = f.read()
    
    missing_vars = []
    for var in required_vars:
        if var not in content or f"{var}=" not in content:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("✅ Environment configuration is complete")
    return True

def start_mcp_server():
    """Start the MCP server in the background"""
    print("🚀 Starting MCP server...")
    process = subprocess.Popen([
        sys.executable, "mcp_server.py"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Give the server a moment to start
    time.sleep(2)
    
    if process.poll() is None:
        print("✅ MCP server started successfully")
        return process
    else:
        stdout, stderr = process.communicate()
        print(f"❌ MCP server failed to start")
        print(f"Error: {stderr.decode()}")
        return None

def start_chainlit():
    """Start the Chainlit application"""
    print("🚀 Starting Chainlit application...")
    print("📱 Application will be available at: http://localhost:8000")
    print("🔌 MCP server will auto-connect on startup")
    print("🧩 Using mcp_config.json to register the Databricks MCP server")

    subprocess.run([
        sys.executable,
        "-m",
        "chainlit",
        "run",
        "app.py",
        "-m",
        "mcp_config.json",
    ])

def main():
    """Main startup function"""
    print("🔧 Databricks MCP Application Startup")
    print("=" * 40)
    
    # Check prerequisites
    if not check_requirements():
        sys.exit(1)
    
    if not check_env_file():
        sys.exit(1)
    
    try:
        print("🎯 Note: MCP server will be started automatically by Chainlit")
        print("📋 No need to manually start the MCP server")
        
        # Start Chainlit (which will auto-start the MCP server)
        start_chainlit()
        
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
