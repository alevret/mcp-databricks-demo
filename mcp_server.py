import os
from typing import Dict
from dotenv import load_dotenv
from databricks.sql import connect
from databricks.sql.client import Connection
from mcp.server.fastmcp import FastMCP
import requests

# Load environment variables
load_dotenv()

# Get Databricks credentials from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Simple session context storage - just store all interactions
interaction_history = []

# Set up the MCP server
mcp = FastMCP("Databricks API Explorer")

# Helper function to get a Databricks SQL connection
def get_databricks_connection() -> Connection:
    """Create and return a Databricks SQL connection with enhanced error handling"""
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH]):
        missing_vars = []
        if not DATABRICKS_HOST: missing_vars.append("DATABRICKS_HOST")
        if not DATABRICKS_TOKEN: missing_vars.append("DATABRICKS_TOKEN") 
        if not DATABRICKS_HTTP_PATH: missing_vars.append("DATABRICKS_HTTP_PATH")
        raise ValueError(f"Missing required Databricks connection details: {', '.join(missing_vars)}")

    try:
        connection = connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        # Test the connection
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return connection
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Databricks: {str(e)}. Please verify your credentials and network connectivity.")

# Helper function for Databricks REST API requests
def databricks_api_request(endpoint: str, method: str = "GET", data: Dict = None) -> Dict:
    """Make a request to the Databricks REST API"""
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN]):
        raise ValueError("Missing required Databricks API credentials in .env file")
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    url = f"https://{DATABRICKS_HOST}/api/2.0/{endpoint}"
    
    if method.upper() == "GET":
        response = requests.get(url, headers=headers)
    elif method.upper() == "POST":
        response = requests.post(url, headers=headers, json=data)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    response.raise_for_status()
    return response.json()

@mcp.resource("schema://tables")
def get_schema() -> str:
    """Provide the list of tables in the Databricks SQL warehouse as a resource"""
    conn = None
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        tables = cursor.tables().fetchall()
        
        table_info = []
        for table in tables:
            table_info.append(f"Database: {table.TABLE_CAT}, Schema: {table.TABLE_SCHEM}, Table: {table.TABLE_NAME}")
        
        return "\n".join(table_info)
    except Exception as e:
        return f"Error retrieving tables: {str(e)}"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                print(f"Warning: Failed to close connection: {e}")

@mcp.tool()
def run_sql_query(sql: str) -> str:
    """Execute SQL queries on Databricks SQL warehouse with safety checks"""
    conn = None
    try:
        # Basic SQL safety checks
        sql_upper = sql.upper().strip()
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE']
        
        # Allow only SELECT and SHOW statements for safety
        if not (sql_upper.startswith('SELECT') or sql_upper.startswith('SHOW') or sql_upper.startswith('DESCRIBE')):
            return f"Error: Only SELECT, SHOW, and DESCRIBE statements are allowed for safety. Detected: {sql_upper.split()[0] if sql_upper else 'empty query'}"
        
        # Check for dangerous keywords
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return f"Error: Query contains potentially dangerous keyword '{keyword}'. Only read operations are allowed."
        
        # Limit query length
        if len(sql) > 10000:
            return "Error: Query too long. Please limit queries to 10,000 characters."
        
        conn = get_databricks_connection()
        cursor = conn.cursor()
        result = cursor.execute(sql)
        
        if result.description:
            # Get column names
            columns = [col[0] for col in result.description]
            
            # Format the result as a table with row limit
            rows = result.fetchmany(1000)  # Limit to 1000 rows for performance
            if not rows:
                result_text = "Query executed successfully. No results returned."
            else:
                # Format as markdown table
                table = "| " + " | ".join(columns) + " |\n"
                table += "| " + " | ".join(["---" for _ in columns]) + " |\n"
                
                for row in rows:
                    # Safely convert each cell to string and handle None values
                    safe_row = [str(cell) if cell is not None else "NULL" for cell in row]
                    table += "| " + " | ".join(safe_row) + " |\n"
                
                # Check if there might be more rows
                if len(rows) == 1000:
                    table += "\n*Note: Results limited to 1000 rows for performance.*\n"
                
                result_text = table
        else:
            result_text = "Query executed successfully. No results returned."
        
        # Store interaction in history
        interaction_history.append({
            'type': 'sql_query',
            'input': sql,
            'output': result_text
        })
        
        return result_text
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}"
        # Store error in history too
        interaction_history.append({
            'type': 'sql_query',
            'input': sql,
            'output': error_msg
        })
        return error_msg
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                print(f"Warning: Failed to close connection: {e}")

@mcp.tool()
def list_jobs() -> str:
    """List all Databricks jobs"""
    try:
        response = databricks_api_request("jobs/list")
        
        if not response.get("jobs"):
            result = "No jobs found."
        else:
            jobs = response.get("jobs", [])
            
            # Format as markdown table
            table = "| Job ID | Job Name | Created By |\n"
            table += "| ------ | -------- | ---------- |\n"
            
            for job in jobs:
                job_id = job.get("job_id", "N/A")
                job_name = job.get("settings", {}).get("name", "N/A")
                created_by = job.get("created_by", "N/A")
                
                table += f"| {job_id} | {job_name} | {created_by} |\n"
            
            result = table
        
        # Store interaction in history
        interaction_history.append({
            'type': 'list_jobs',
            'input': 'list_jobs()',
            'output': result
        })
        
        return result
    except Exception as e:
        error_msg = f"Error listing jobs: {str(e)}"
        interaction_history.append({
            'type': 'list_jobs',
            'input': 'list_jobs()',
            'output': error_msg
        })
        return error_msg

@mcp.tool()
def get_job_status(job_id: int) -> str:
    """Get the status of a specific Databricks job"""
    try:
        response = databricks_api_request("jobs/runs/list", data={"job_id": job_id})
        
        if not response.get("runs"):
            result = f"No runs found for job ID {job_id}."
        else:
            runs = response.get("runs", [])
            
            # Format as markdown table
            table = "| Run ID | State | Start Time | End Time | Duration |\n"
            table += "| ------ | ----- | ---------- | -------- | -------- |\n"
            
            for run in runs:
                run_id = run.get("run_id", "N/A")
                state = run.get("state", {}).get("result_state", "N/A")
                
                # Convert timestamps to readable format if they exist
                start_time = run.get("start_time", 0)
                end_time = run.get("end_time", 0)
                
                if start_time and end_time:
                    duration = f"{(end_time - start_time) / 1000:.2f}s"
                else:
                    duration = "N/A"
                
                # Format timestamps
                import datetime
                start_time_str = datetime.datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if start_time else "N/A"
                end_time_str = datetime.datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if end_time else "N/A"
                
                table += f"| {run_id} | {state} | {start_time_str} | {end_time_str} | {duration} |\n"
            
            result = table
        
        # Store interaction in history
        interaction_history.append({
            'type': 'get_job_status',
            'input': f'get_job_status({job_id})',
            'output': result
        })
        
        return result
    except Exception as e:
        error_msg = f"Error getting job status: {str(e)}"
        interaction_history.append({
            'type': 'get_job_status',
            'input': f'get_job_status({job_id})',
            'output': error_msg
        })
        return error_msg

@mcp.tool()
def get_job_details(job_id: int) -> str:
    """Get detailed information about a specific Databricks job"""
    try:
        response = databricks_api_request(f"jobs/get?job_id={job_id}", method="GET")
        
        # Format the job details
        job_name = response.get("settings", {}).get("name", "N/A")
        created_time = response.get("created_time", 0)
        
        # Convert timestamp to readable format
        import datetime
        created_time_str = datetime.datetime.fromtimestamp(created_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if created_time else "N/A"
        
        # Get job tasks
        tasks = response.get("settings", {}).get("tasks", [])
        
        result = f"## Job Details: {job_name}\n\n"
        result += f"- **Job ID:** {job_id}\n"
        result += f"- **Created:** {created_time_str}\n"
        result += f"- **Creator:** {response.get('creator_user_name', 'N/A')}\n\n"
        
        if tasks:
            result += "### Tasks:\n\n"
            result += "| Task Key | Task Type | Description |\n"
            result += "| -------- | --------- | ----------- |\n"
            
            for task in tasks:
                task_key = task.get("task_key", "N/A")
                task_type = next(iter([k for k in task.keys() if k.endswith("_task")]), "N/A")
                description = task.get("description", "N/A")
                
                result += f"| {task_key} | {task_type} | {description} |\n"
        
        # Store interaction in history
        interaction_history.append({
            'type': 'get_job_details',
            'input': f'get_job_details({job_id})',
            'output': result
        })
        
        return result
    except Exception as e:
        error_msg = f"Error getting job details: {str(e)}"
        interaction_history.append({
            'type': 'get_job_details',
            'input': f'get_job_details({job_id})',
            'output': error_msg
        })
        return error_msg

@mcp.tool()
def get_interaction_history() -> str:
    """Get the history of all interactions in this session"""
    if not interaction_history:
        return "No interactions found in this session."
    
    history = "## Interaction History\n\n"
    for i, interaction in enumerate(interaction_history, 1):
        # Debug: print what we're working with
        print(f"DEBUG: Processing interaction {i}")
        print(f"DEBUG: Type - {interaction.get('type', 'UNKNOWN')}")
        print(f"DEBUG: Input type - {type(interaction.get('input', 'UNKNOWN'))}")
        print(f"DEBUG: Output type - {type(interaction.get('output', 'UNKNOWN'))}")
        
        history += f"### {i}. {interaction['type']}\n"
        history += f"**Input:** `{str(interaction['input'])}`\n"
        
        # Ensure output is properly converted to string
        output_raw = interaction.get('output', 'None')
        if output_raw is None:
            output = "None"
        elif isinstance(output_raw, list):
            output = str(output_raw)  # Convert list to string representation
        elif isinstance(output_raw, dict):
            output = str(output_raw)  # Convert dict to string representation  
        else:
            output = str(output_raw)
            
        truncated_output = output[:200] + ('...' if len(output) > 200 else '')
        history += f"**Output:** {truncated_output}\n\n"
    
    return history

@mcp.tool()
def list_databases() -> str:
    """List all databases/catalogs in Databricks"""
    conn = None
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        
        if not databases:
            result = "No databases found."
        else:
            result = "## Available Databases\n\n"
            for db in databases:
                db_name = db[0] if isinstance(db, (list, tuple)) else str(db)
                result += f"- {db_name}\n"
        
        interaction_history.append({
            'type': 'list_databases',
            'input': 'list_databases()',
            'output': result
        })
        
        return result
    except Exception as e:
        error_msg = f"Error listing databases: {str(e)}"
        interaction_history.append({
            'type': 'list_databases',
            'input': 'list_databases()',
            'output': error_msg
        })
        return error_msg
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                print(f"Warning: Failed to close connection: {e}")

@mcp.tool()
def describe_table(table_name: str) -> str:
    """Get detailed schema information for a specific table"""
    conn = None
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        
        # Use DESCRIBE EXTENDED for more detailed information
        cursor.execute(f"DESCRIBE EXTENDED {table_name}")
        description = cursor.fetchall()
        
        if not description:
            result = f"No schema information found for table: {table_name}"
        else:
            result = f"## Schema for table: {table_name}\n\n"
            result += "| Column | Type | Comment |\n"
            result += "| ------ | ---- | ------- |\n"
            
            for row in description:
                if len(row) >= 3:
                    col_name = str(row[0]) if row[0] else "N/A"
                    col_type = str(row[1]) if row[1] else "N/A"  
                    col_comment = str(row[2]) if row[2] else ""
                    result += f"| {col_name} | {col_type} | {col_comment} |\n"
        
        interaction_history.append({
            'type': 'describe_table',
            'input': f'describe_table({table_name})',
            'output': result
        })
        
        return result
    except Exception as e:
        error_msg = f"Error describing table {table_name}: {str(e)}"
        interaction_history.append({
            'type': 'describe_table',
            'input': f'describe_table({table_name})',
            'output': error_msg
        })
        return error_msg
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                print(f"Warning: Failed to close connection: {e}")

@mcp.tool()
def get_cluster_info() -> str:
    """Get information about available Databricks clusters"""
    try:
        response = databricks_api_request("clusters/list")
        
        if not response.get("clusters"):
            result = "No clusters found."
        else:
            clusters = response.get("clusters", [])
            
            result = "## Available Clusters\n\n"
            result += "| Cluster ID | Cluster Name | State | Node Type |\n"
            result += "| ---------- | ------------ | ----- | --------- |\n"
            
            for cluster in clusters:
                cluster_id = cluster.get("cluster_id", "N/A")
                cluster_name = cluster.get("cluster_name", "N/A")
                state = cluster.get("state", "N/A")
                node_type = cluster.get("node_type_id", "N/A")
                
                result += f"| {cluster_id} | {cluster_name} | {state} | {node_type} |\n"
        
        interaction_history.append({
            'type': 'get_cluster_info',
            'input': 'get_cluster_info()',
            'output': result
        })
        
        return result
    except Exception as e:
        error_msg = f"Error getting cluster information: {str(e)}"
        interaction_history.append({
            'type': 'get_cluster_info',
            'input': 'get_cluster_info()',
            'output': error_msg
        })
        return error_msg

if __name__ == "__main__":

    transport = os.getenv("MCP_TRANSPORT", "stdio").lower()
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    path = os.getenv("MCP_PATH", "/mcp")

    if transport in ("http", "sse"):
        # set server configuration before running
        mcp.settings.host = host
        mcp.settings.port = port
        mcp.settings.path = path
        mcp.run(transport=transport)
    else:
        mcp.run()