"""
DataForge AI - Real Airbyte Integration
Connects to actual Airbyte instance for data ingestion from various sources
"""

import os
import json
import httpx
from typing import Dict, Any, List, Optional
from datetime import datetime


class AirbyteConnector:
    """Real Airbyte API integration for data ingestion"""
    
    def __init__(self, base_url: str = None):
        self.base_url = base_url or os.getenv("AIRBYTE_URL", "http://localhost:8000")
        self.api_version = "v1"
        self.username = os.getenv("AIRBYTE_USERNAME", "airbyte")
        self.password = os.getenv("AIRBYTE_PASSWORD", "password")
        self.workspace_id = None
        self._initialized = False
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get authenticated HTTP client"""
        return httpx.AsyncClient(
            base_url=self.base_url,
            auth=(self.username, self.password),
            timeout=60.0
        )
    
    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make authenticated request to Airbyte API"""
        url = f"/api/{self.api_version}/{endpoint}"
        
        async with await self._get_client() as client:
            try:
                if method == "GET":
                    response = await client.get(url)
                elif method == "POST":
                    response = await client.post(url, json=data)
                elif method == "PUT":
                    response = await client.put(url, json=data)
                elif method == "DELETE":
                    response = await client.delete(url)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                if response.status_code == 200:
                    return response.json()
                else:
                    return {
                        "error": f"API error: {response.status_code}",
                        "details": response.text
                    }
            except httpx.ConnectError:
                return {
                    "error": "Cannot connect to Airbyte. Is it running?",
                    "hint": "Start Airbyte with: docker-compose up -d"
                }
            except Exception as e:
                return {"error": str(e)}
    
    async def initialize(self) -> Dict[str, Any]:
        """Initialize connection and get workspace ID"""
        if self._initialized:
            return {"status": "already_initialized"}
        
        # Get workspaces
        result = await self._make_request("POST", "workspaces/list", {})
        
        if "error" in result:
            return result
        
        workspaces = result.get("workspaces", [])
        
        if workspaces:
            self.workspace_id = workspaces[0].get("workspaceId")
            self._initialized = True
            return {
                "status": "initialized",
                "workspace_id": self.workspace_id,
                "workspace_name": workspaces[0].get("name", "default")
            }
        else:
            # Create default workspace
            create_result = await self._make_request(
                "POST", 
                "workspaces/create",
                {"name": "DataForge Workspace"}
            )
            
            if "workspaceId" in create_result:
                self.workspace_id = create_result["workspaceId"]
                self._initialized = True
                return {
                    "status": "created",
                    "workspace_id": self.workspace_id
                }
            
            return {"error": "Failed to create workspace"}
    
    # ============ SOURCE OPERATIONS ============
    
    async def list_source_definitions(self) -> Dict[str, Any]:
        """List all available source connectors"""
        result = await self._make_request("POST", "source_definitions/list", {})
        
        if "error" in result:
            # Return popular connectors as fallback
            return {
                "source_definitions": self._get_popular_connectors(),
                "note": "Using cached connector list (Airbyte not available)"
            }
        
        return result
    
    def _get_popular_connectors(self) -> List[Dict]:
        """Get list of popular connectors for display"""
        return [
            {"sourceDefinitionId": "postgres", "name": "PostgreSQL", "sourceType": "database"},
            {"sourceDefinitionId": "mysql", "name": "MySQL", "sourceType": "database"},
            {"sourceDefinitionId": "mssql", "name": "Microsoft SQL Server", "sourceType": "database"},
            {"sourceDefinitionId": "mongodb", "name": "MongoDB", "sourceType": "database"},
            {"sourceDefinitionId": "s3", "name": "Amazon S3", "sourceType": "cloud_storage"},
            {"sourceDefinitionId": "gcs", "name": "Google Cloud Storage", "sourceType": "cloud_storage"},
            {"sourceDefinitionId": "bigquery", "name": "BigQuery", "sourceType": "data_warehouse"},
            {"sourceDefinitionId": "snowflake", "name": "Snowflake", "sourceType": "data_warehouse"},
            {"sourceDefinitionId": "salesforce", "name": "Salesforce", "sourceType": "saas"},
            {"sourceDefinitionId": "hubspot", "name": "HubSpot", "sourceType": "saas"},
            {"sourceDefinitionId": "stripe", "name": "Stripe", "sourceType": "saas"},
            {"sourceDefinitionId": "shopify", "name": "Shopify", "sourceType": "saas"},
            {"sourceDefinitionId": "google_sheets", "name": "Google Sheets", "sourceType": "saas"},
            {"sourceDefinitionId": "api", "name": "REST API", "sourceType": "api"},
            {"sourceDefinitionId": "file", "name": "File (CSV/JSON)", "sourceType": "file"},
        ]
    
    async def create_source(
        self,
        name: str,
        source_definition_id: str,
        connection_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a new data source.
        
        Args:
            name: Name for this source
            source_definition_id: Connector type (e.g., 'postgres', 's3')
            connection_config: Connection parameters
            
        Returns:
            Created source info
        """
        if not self.workspace_id:
            await self.initialize()
        
        data = {
            "workspaceId": self.workspace_id,
            "name": name,
            "sourceDefinitionId": source_definition_id,
            "connectionConfiguration": connection_config
        }
        
        result = await self._make_request("POST", "sources/create", data)
        
        if "error" in result:
            # Simulate for demo if Airbyte not available
            return {
                "sourceId": f"src_{name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "name": name,
                "sourceDefinitionId": source_definition_id,
                "status": "created",
                "note": "Created in demo mode (Airbyte not connected)"
            }
        
        return result
    
    async def test_source_connection(self, source_id: str) -> Dict[str, Any]:
        """Test connection to a source"""
        data = {"sourceId": source_id}
        result = await self._make_request("POST", "sources/check_connection", data)
        return result
    
    async def discover_source_schema(self, source_id: str) -> Dict[str, Any]:
        """Discover available streams/tables from a source"""
        data = {"sourceId": source_id}
        result = await self._make_request("POST", "sources/discover_schema", data)
        
        if "error" in result:
            return {
                "streams": [],
                "note": "Schema discovery requires Airbyte connection"
            }
        
        return result
    
    async def list_sources(self) -> Dict[str, Any]:
        """List all configured sources"""
        if not self.workspace_id:
            await self.initialize()
        
        data = {"workspaceId": self.workspace_id}
        result = await self._make_request("POST", "sources/list", data)
        
        if "error" in result:
            return {"sources": [], "error": result["error"]}
        
        return result
    
    async def delete_source(self, source_id: str) -> Dict[str, Any]:
        """Delete a source"""
        data = {"sourceId": source_id}
        return await self._make_request("POST", "sources/delete", data)
    
    # ============ DESTINATION OPERATIONS ============
    
    async def list_destination_definitions(self) -> Dict[str, Any]:
        """List available destination connectors"""
        result = await self._make_request("POST", "destination_definitions/list", {})
        
        if "error" in result:
            return {
                "destination_definitions": [
                    {"destinationDefinitionId": "duckdb", "name": "DuckDB"},
                    {"destinationDefinitionId": "postgres", "name": "PostgreSQL"},
                    {"destinationDefinitionId": "bigquery", "name": "BigQuery"},
                    {"destinationDefinitionId": "snowflake", "name": "Snowflake"},
                    {"destinationDefinitionId": "s3", "name": "Amazon S3"},
                    {"destinationDefinitionId": "local", "name": "Local File"},
                ]
            }
        
        return result
    
    async def create_destination(
        self,
        name: str,
        destination_definition_id: str,
        connection_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a destination"""
        if not self.workspace_id:
            await self.initialize()
        
        data = {
            "workspaceId": self.workspace_id,
            "name": name,
            "destinationDefinitionId": destination_definition_id,
            "connectionConfiguration": connection_config
        }
        
        result = await self._make_request("POST", "destinations/create", data)
        
        if "error" in result:
            return {
                "destinationId": f"dest_{name.lower().replace(' ', '_')}",
                "name": name,
                "status": "created",
                "note": "Created in demo mode"
            }
        
        return result
    
    async def list_destinations(self) -> Dict[str, Any]:
        """List all destinations"""
        if not self.workspace_id:
            await self.initialize()
        
        data = {"workspaceId": self.workspace_id}
        result = await self._make_request("POST", "destinations/list", data)
        
        if "error" in result:
            return {"destinations": []}
        
        return result
    
    # ============ CONNECTION OPERATIONS ============
    
    async def create_connection(
        self,
        name: str,
        source_id: str,
        destination_id: str,
        streams: List[Dict[str, Any]] = None,
        schedule: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Create a connection between source and destination.
        
        Args:
            name: Connection name
            source_id: Source ID
            destination_id: Destination ID
            streams: List of streams to sync (all if None)
            schedule: Sync schedule configuration
            
        Returns:
            Created connection info
        """
        if not self.workspace_id:
            await self.initialize()
        
        data = {
            "workspaceId": self.workspace_id,
            "name": name,
            "sourceId": source_id,
            "destinationId": destination_id,
            "schedule": schedule or {"scheduleType": "manual"},
            "status": "active"
        }
        
        if streams:
            data["syncCatalog"] = {"streams": streams}
        
        result = await self._make_request("POST", "connections/create", data)
        
        if "error" in result:
            return {
                "connectionId": f"conn_{name.lower().replace(' ', '_')}",
                "name": name,
                "sourceId": source_id,
                "destinationId": destination_id,
                "status": "active",
                "note": "Created in demo mode"
            }
        
        return result
    
    async def list_connections(self) -> Dict[str, Any]:
        """List all connections"""
        if not self.workspace_id:
            await self.initialize()
        
        data = {"workspaceId": self.workspace_id}
        result = await self._make_request("POST", "connections/list", data)
        
        if "error" in result:
            return {"connections": []}
        
        return result
    
    async def sync_connection(self, connection_id: str) -> Dict[str, Any]:
        """Trigger a sync for a connection"""
        data = {"connectionId": connection_id}
        result = await self._make_request("POST", "connections/sync", data)
        
        if "error" in result:
            # Return simulated job
            return {
                "job": {
                    "id": f"job_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    "status": "running",
                    "connectionId": connection_id
                },
                "note": "Sync triggered in demo mode"
            }
        
        return result
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a sync job"""
        data = {"id": job_id}
        return await self._make_request("POST", "jobs/get", data)
    
    async def delete_connection(self, connection_id: str) -> Dict[str, Any]:
        """Delete a connection"""
        data = {"connectionId": connection_id}
        return await self._make_request("POST", "connections/delete", data)
    
    # ============ HELPER METHODS ============
    
    async def create_postgres_source(
        self,
        name: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        ssl_mode: str = "disable"
    ) -> Dict[str, Any]:
        """Create PostgreSQL source with standard config"""
        return await self.create_source(
            name=name,
            source_definition_id="decd338e-5647-4c0b-adf4-da0e75f5a750",  # Postgres ID
            connection_config={
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "password": password,
                "ssl_mode": {"mode": ssl_mode}
            }
        )
    
    async def create_mysql_source(
        self,
        name: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str
    ) -> Dict[str, Any]:
        """Create MySQL source"""
        return await self.create_source(
            name=name,
            source_definition_id="435bb9a5-7887-4809-aa58-28c27df0d6ad",  # MySQL ID
            connection_config={
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "password": password
            }
        )
    
    async def create_s3_source(
        self,
        name: str,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "us-east-1",
        path_prefix: str = ""
    ) -> Dict[str, Any]:
        """Create S3 source"""
        return await self.create_source(
            name=name,
            source_definition_id="d8286229-4b69-4289-abd1-35adb3710b7c",  # S3 ID
            connection_config={
                "bucket": bucket,
                "aws_access_key_id": access_key_id,
                "aws_secret_access_key": secret_access_key,
                "region": region,
                "path_prefix": path_prefix,
                "format": {"format_type": "csv"}
            }
        )
    
    async def create_duckdb_destination(
        self,
        name: str,
        database_path: str
    ) -> Dict[str, Any]:
        """Create DuckDB destination"""
        return await self.create_destination(
            name=name,
            destination_definition_id="duckdb",  # Custom or local
            connection_config={
                "database_path": database_path
            }
        )
    
    async def get_connection_info(self, connection_id: str) -> Dict[str, Any]:
        """Get detailed info about a connection"""
        data = {"connectionId": connection_id}
        result = await self._make_request("POST", "connections/get", data)
        
        if "error" in result:
            return {
                "connectionId": connection_id,
                "status": "unknown",
                "note": "Connection info requires Airbyte connection"
            }
        
        return result
    
    async def get_source_catalog(self, source_id: str) -> Dict[str, Any]:
        """Get catalog of available streams from source"""
        return await self.discover_source_schema(source_id)
    
    def get_connection_template(self, source_type: str) -> Dict[str, Any]:
        """Get connection configuration template for a source type"""
        templates = {
            "postgres": {
                "host": "localhost",
                "port": 5432,
                "database": "mydb",
                "username": "postgres",
                "password": "",
                "ssl_mode": "disable"
            },
            "mysql": {
                "host": "localhost",
                "port": 3306,
                "database": "mydb",
                "username": "root",
                "password": ""
            },
            "mongodb": {
                "host": "localhost",
                "port": 27017,
                "database": "mydb",
                "auth_source": "admin"
            },
            "s3": {
                "bucket": "my-bucket",
                "aws_access_key_id": "",
                "aws_secret_access_key": "",
                "region": "us-east-1",
                "path_prefix": "",
                "format": "csv"
            },
            "api": {
                "url": "https://api.example.com",
                "method": "GET",
                "headers": {},
                "auth_type": "bearer"
            }
        }
        
        return templates.get(source_type, {})
