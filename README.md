# Azure OpenAI MCP Integration with Databricks

A Chainlit-based chat interface that connects Azure OpenAI API with Databricks through the Model Context Protocol (MCP). This application provides a natural language interface to interact with your Databricks workspace, execute SQL queries, manage jobs, and monitor infrastructure.

## 📋 Prerequisites

- **Azure OpenAI API**: Valid endpoint, API key, and model deployment
- **Databricks Workspace**: Active workspace with SQL warehouse configured
- **Python Environment**: Python 3.8 or higher
- **Network Access**: Connectivity to both Azure OpenAI and Databricks services

## 🚀 Quick Start

###  Automated Setup
```bash
# Clone the repository
git clone <repository-url>
cd mcp_aoai

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# On Windows
.venv\Scripts\activate
# On macOS/Linux
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create and configure .env file (see configuration section)

# Then run the automated startup
python start.py
```

## ⚙️ Configuration

Create a `.env` file in the project root with the following required variables:

```env
# Databricks Configuration
DATABRICKS_HOST="your_databricks_host"
DATABRICKS_TOKEN="your_access_token"
DATABRICKS_HTTP_PATH="your_warehouse_http_path"

# Azure OpenAI Configuration
AZURE_OPENAI_ENDPOINT="your_endpoint_here"
AZURE_OPENAI_API_KEY="your_api_key_here"
AZURE_OPENAI_MODEL="your_deployment_name_here"
AZURE_OPENAI_API_VERSION="2025-04-01-preview"
```

### Getting Databricks Credentials
1. **Host**: Your Databricks workspace URL (e.g., `adb-123456789.1.azuredatabricks.net`)
2. **Token**: Personal Access Token from User Settings → Developer → Access Tokens
3. **HTTP Path**: SQL Warehouse connection details → Advanced options → JDBC/ODBC

### Azure OpenAI Setup
1. Create an Azure OpenAI resource in Azure
2. Deploy a model (e.g. I'm using gpt-4.1-nano)
3. Get endpoint and API key from the resource overview

### Sample Data Setup (Optional)
To test the application with sample data, you can create a table with building sensor readings:

1. **Create Sample Table**: Execute the SQL commands in `sample.sql` to create a `building_sensor_readings` table with 50 rows of sample data
2. **Table Structure**: The table includes sensor readings from different floors and rooms with temperature data and timestamps
3. **Usage**: Perfect for testing queries like temperature analysis, sensor monitoring, and data exploration features

```sql
-- Execute the contents of sample.sql in your Databricks SQL warehouse
-- This creates: default.building_sensor_readings table with sensor data
```

The sample data includes:
- **4 sensors** across 3 floors (Lobby, Conference Rooms, Server Room, Office)
- **Temperature readings** in Celsius with timestamps
- **54 data points** for comprehensive testing scenarios

## 🛠️ Available Tools & Capabilities

The MCP server provides 8 powerful tools for Databricks interaction:

### 📊 Data Operations
- **`run_sql_query(sql)`** - Execute SELECT, SHOW, or DESCRIBE queries with safety validation
  - Supports markdown table formatting
  - Results limited to 1000 rows for performance
  - Automatic SQL injection protection
- **`list_databases()`** - List all available databases/catalogs in the workspace
- **`describe_table(table_name)`** - Get comprehensive schema information for any table
- **`get_schema()`** - Resource providing complete warehouse table structure

### ⚙️ Job Management
- **`list_jobs()`** - Display all Databricks jobs with metadata (ID, name, creator)
- **`get_job_status(job_id)`** - Detailed run history and status for specific jobs
- **`get_job_details(job_id)`** - Complete job configuration and task breakdown

### 🏗️ Infrastructure & Monitoring  
- **`get_cluster_info()`** - List all clusters with state and configuration details
- **`get_interaction_history()`** - Session interaction tracking for debugging and audit

### 🔐 Safety Features
- **Query Validation**: Only SELECT, SHOW, and DESCRIBE statements permitted
- **Result Limiting**: Automatic pagination and row limits 
- **Connection Management**: Robust connection pooling and cleanup
- **Error Handling**: Comprehensive error reporting with context
- **Session Tracking**: Complete audit trail of all interactions

## 🎯 Usage Guide

### Automated Startup (Recommended)
The `start.py` script provides automated environment validation and startup:

```bash
python start.py
```

**Features of start.py:**
- ✅ Validates all required packages are installed
- ✅ Checks `.env` file configuration completeness  
- ✅ Provides helpful error messages for missing components
- ✅ Automatically starts Chainlit with proper MCP configuration
- ✅ Opens browser to the application interface

### Manual Connection (Alternative)
If using `chainlit run app.py` directly:

1. **Start Application**: `chainlit run app.py`
2. **Connect MCP Server**:
   - Click "MCP Servers" button in the Chainlit interface
   - Click "Connect an MCP"
   - Enter configuration:
     - **Name**: `databricks`
     - **Type**: `stdio`  
     - **Command**: `python mcp_server.py`
   - Click "Confirm"
3. **Verify Connection**: Look for welcome message listing 8 available tools

### Troubleshooting Connection Issues
- Ensure all environment variables are properly set
- Verify Databricks connectivity with a simple test query
- Check Azure OpenAI API key and model deployment status
- Review terminal output for detailed error messages

## 💬 Example Interactions

Here are practical examples of how to interact with your Databricks environment:

### Database & Schema Exploration
```
"What databases are available in my workspace?"
"Show me all tables in the sales_analytics database"
"Describe the schema for the customer_data table"
"What columns does the orders table have?"
```

### Data Querying
```
"Show me the first 10 rows from the sales_data table"
"Count the number of records in customer_transactions"
"What are the unique values in the status column?"
"Show recent orders from the last 7 days"
```

### Job Management & Monitoring
```
"List all jobs in my workspace"
"What's the status of job 12345?"
"Show me details for the daily_ETL job"
"Which jobs failed in the last run?"
```

### Infrastructure Management
```
"What clusters are available?"
"Show me cluster information and their current status"
"Which clusters are currently running?"
```

### Session & Debugging
```
"Show me my interaction history"
"What queries have I run in this session?"
"Debug my last query"
```

### Complex Analytical Queries
```
"Analyze sales trends by region in the last quarter"
"Compare performance metrics between different product categories"
"Show me the top 5 customers by total purchase amount"
```

## 🏗️ Architecture & Components

### Core Components
- **`app.py`**: Main Chainlit application with Azure OpenAI integration
- **`mcp_server.py`**: FastMCP server implementing all Databricks tools
- **`start.py`**: Automated startup script with environment validation
- **`mcp_config.json`**: MCP server configuration for Chainlit integration

### Key Classes & Functions
- **`ChatClient`**: Manages Azure OpenAI interactions and message streaming
- **`get_databricks_connection()`**: Robust connection management with error handling
- **`databricks_api_request()`**: Unified REST API client for Databricks services
- **`interaction_history`**: Session-level tracking for debugging and audit

### Security & Safety Architecture
- **SQL Injection Prevention**: Whitelist-based query validation
- **Query Type Restriction**: Only SELECT, SHOW, DESCRIBE operations
- **Result Set Limiting**: Automatic pagination and row count limits
- **Connection Lifecycle**: Proper resource cleanup and error handling
- **Error Isolation**: Comprehensive exception handling with user-friendly messages

### Integration Flow
1. **User Input** → Chainlit Interface
2. **Message Processing** → Azure OpenAI API  
3. **Tool Calls** → MCP Server (FastMCP)
4. **Databricks Interaction** → SQL Warehouse / REST API
5. **Response Formatting** → Markdown Tables / Structured Output
6. **Session Tracking** → Interaction History Storage

## 🔧 Configuration & Customization

### Chainlit Configuration
The application supports extensive customization through `config.toml`:
- **Chain of Thought Display**: Full CoT mode enabled for transparency
- **Custom Styling**: CSS and JavaScript injection support
- **Header Customization**: Custom navigation links and branding

### Welcome Screen
Modify `chainlit.md` to customize the application welcome screen and instructions.

### Environment Validation
The `start.py` script includes comprehensive environment checking:
- **Package Dependencies**: Validates all required Python packages
- **Environment Variables**: Ensures all `.env` variables are present
- **Connection Testing**: Optional connectivity verification

### MCP Server Configuration
The `mcp_config.json` file defines the MCP server setup:
```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["mcp_server.py"],
      "cwd": "."
    }
  }
}
```

## 🚨 Troubleshooting

### Common Issues

**Connection Errors**
- Verify Databricks token and workspace URL
- Check network connectivity to Databricks
- Ensure SQL warehouse is running

**Azure OpenAI Issues**  
- Confirm API key and endpoint are correct
- Verify model deployment exists and is active
- Check API version compatibility

**MCP Server Problems**
- Ensure Python dependencies are installed
- Check for port conflicts
- Review terminal output for detailed error messages

**Query Execution Failures**
- Verify table and database names exist
- Check SQL syntax for SELECT/SHOW/DESCRIBE statements
- Review query length limits (10,000 characters max)

### Debug Mode
Enable detailed logging by checking terminal output when running with `start.py` or manually starting the application.

## 📚 Dependencies

### Core Requirements (from requirements.txt)
```
chainlit                 # Web interface framework
python-dotenv           # Environment variable management
openai                  # Azure OpenAI API client
mcp                     # Model Context Protocol
aiohttp                 # Async HTTP client
databricks-sql-connector # Databricks SQL connectivity
requests                # HTTP requests for REST API
fastmcp                 # Fast MCP server implementation
```

### Optional Dependencies
- **Debugging Tools**: Built-in interaction history tracking
- **Development**: All dependencies included for local development

## 🤝 Contributing & Development

### Development Setup
1. Fork the repository
2. Set up development environment with all dependencies
3. Configure test environment variables
4. Run tests with `python test_integration.py`

### Code Structure
- Follow existing code patterns for new tools
- Maintain comprehensive error handling
- Add interaction history tracking for new functions
- Include safety validation for any database operations

### Testing
The project includes integration testing capabilities:
- **`test_integration.py`**: Comprehensive testing framework
- **`debug_mcp.py`**: MCP server debugging utilities

## 📄 License & Support

### Documentation Resources
- **[Chainlit Documentation](https://docs.chainlit.io)** - Web interface framework
- **[Azure OpenAI Documentation](https://learn.microsoft.com/azure/cognitive-services/openai/)** - AI service setup
- **[Databricks SQL Documentation](https://docs.databricks.com/sql/)** - SQL warehouse configuration
- **[Model Context Protocol](https://modelcontextprotocol.io/)** - MCP specification and tools

### Community & Support
- GitHub Issues for bug reports and feature requests
- Documentation updates and improvements welcome
- Community contributions encouraged

---

**Built with ❤️ using Chainlit, Azure OpenAI, and Databricks**
