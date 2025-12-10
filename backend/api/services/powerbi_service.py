# backend/api/services/powerbi_service.py

"""
Power BI Service for fetching data from Power BI reports.

Requirements:
- Azure AD app registration for authentication
- Power BI Pro/Premium license
- Report access for hello@growthnity.com

Setup:
1. Register app in Azure AD: https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps
2. Add API permissions: Power BI Service → Report.Read.All
3. Get: tenant_id, client_id, client_secret
4. Add to settings.py or environment variables
"""

import requests
import pandas as pd
from io import StringIO
from django.conf import settings


class PowerBIService:
    """Service to interact with Power BI REST API."""
    
    # Power BI API base URL
    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    
    # Authentication endpoint
    AUTH_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    def __init__(self):
        # Get credentials from settings
        self.tenant_id = getattr(settings, 'POWERBI_TENANT_ID', None)
        self.client_id = getattr(settings, 'POWERBI_CLIENT_ID', None)
        self.client_secret = getattr(settings, 'POWERBI_CLIENT_SECRET', None)
        self.username = getattr(settings, 'POWERBI_USERNAME', 'hello@growthnity.com')
        self.password = getattr(settings, 'POWERBI_PASSWORD', None)
        
        self._access_token = None
    
    def _get_access_token(self):
        """
        Get OAuth2 access token for Power BI API.
        
        Two authentication methods:
        1. Service Principal (client_credentials) - recommended for automation
        2. User credentials (password grant) - requires username/password
        """
        if self._access_token:
            return self._access_token
        
        if not self.tenant_id:
            raise ValueError("POWERBI_TENANT_ID not configured in settings")
        
        auth_url = self.AUTH_URL.format(tenant_id=self.tenant_id)
        
        # Method 1: Service Principal (recommended)
        if self.client_id and self.client_secret:
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': 'https://analysis.windows.net/powerbi/api/.default'
            }
        # Method 2: User credentials (fallback)
        elif self.username and self.password:
            data = {
                'grant_type': 'password',
                'client_id': self.client_id,
                'username': self.username,
                'password': self.password,
                'scope': 'https://analysis.windows.net/powerbi/api/.default'
            }
        else:
            raise ValueError("Power BI credentials not configured. Need either (client_id + client_secret) or (username + password)")
        
        response = requests.post(auth_url, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        self._access_token = token_data['access_token']
        return self._access_token
    
    def _get_headers(self):
        """Get authorization headers with bearer token."""
        token = self._get_access_token()
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def export_report_to_csv(self, group_id, report_id, page_name=None):
        """
        Export Power BI report data to CSV.
        
        Args:
            group_id: Workspace/Group ID (from URL: /groups/{group_id}/)
            report_id: Report ID (from URL: /reports/{report_id}/)
            page_name: Optional specific page name to export
        
        Returns:
            pandas DataFrame
        
        Note: Power BI export API has limitations:
        - Maximum 150,000 rows per export
        - Export format can be PDF, PPTX, PNG, or via DAX query
        - For data extraction, we'll use executeQueries API with DAX
        """
        headers = self._get_headers()
        
        # Power BI export endpoint
        export_url = f"{self.BASE_URL}/groups/{group_id}/reports/{report_id}/ExportTo"
        
        # Request export to file
        export_request = {
            "format": "CSV",  # Note: CSV not directly supported, using XLSX then convert
        }
        
        if page_name:
            export_request["powerBIReportConfiguration"] = {
                "pages": [{"pageName": page_name}]
            }
        
        # Initiate export
        response = requests.post(export_url, json=export_request, headers=headers)
        response.raise_for_status()
        
        export_id = response.json()['id']
        
        # Poll for export completion
        status_url = f"{export_url}/{export_id}"
        import time
        max_attempts = 30
        
        for attempt in range(max_attempts):
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            status_data = status_response.json()
            
            if status_data['status'] == 'Succeeded':
                # Download file
                file_url = f"{status_url}/file"
                file_response = requests.get(file_url, headers=headers)
                file_response.raise_for_status()
                
                # Convert to DataFrame (assuming XLSX format)
                df = pd.read_excel(StringIO(file_response.content))
                return df
            
            elif status_data['status'] == 'Failed':
                raise Exception(f"Export failed: {status_data.get('error')}")
            
            time.sleep(2)  # Wait 2 seconds before checking again
        
        raise TimeoutError("Export timed out after 60 seconds")
    
    def execute_dax_query(self, group_id, dataset_id, dax_query):
        """
        Execute DAX query on Power BI dataset to extract data.
        
        This is the recommended method for data extraction as it:
        - Returns data directly as JSON
        - Supports filtering and aggregation
        - No row limit (within reason)
        
        Args:
            group_id: Workspace/Group ID
            dataset_id: Dataset ID (not report ID)
            dax_query: DAX query string
        
        Returns:
            pandas DataFrame
        
        Example DAX query:
            EVALUATE
            SUMMARIZECOLUMNS(
                'Table'[Column1],
                'Table'[Column2],
                "Sum", SUM('Table'[Value])
            )
        """
        headers = self._get_headers()
        
        # executeQueries endpoint
        query_url = f"{self.BASE_URL}/groups/{group_id}/datasets/{dataset_id}/executeQueries"
        
        query_request = {
            "queries": [
                {
                    "query": dax_query
                }
            ],
            "serializerSettings": {
                "includeNulls": True
            }
        }
        
        response = requests.post(query_url, json=query_request, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        
        # Parse result into DataFrame
        if 'results' in result and len(result['results']) > 0:
            tables = result['results'][0].get('tables', [])
            if tables and len(tables) > 0:
                rows = tables[0].get('rows', [])
                df = pd.DataFrame(rows)
                return df
        
        return pd.DataFrame()  # Empty if no data
    
    def get_dataset_id_from_report(self, group_id, report_id):
        """
        Get the dataset ID associated with a report.
        
        Needed for execute_dax_query method.
        """
        headers = self._get_headers()
        
        report_url = f"{self.BASE_URL}/groups/{group_id}/reports/{report_id}"
        response = requests.get(report_url, headers=headers)
        response.raise_for_status()
        
        report_data = response.json()
        return report_data.get('datasetId')
    
    def list_datasets(self, group_id):
        """List all datasets in a workspace."""
        headers = self._get_headers()
        
        datasets_url = f"{self.BASE_URL}/groups/{group_id}/datasets"
        response = requests.get(datasets_url, headers=headers)
        response.raise_for_status()
        
        return response.json().get('value', [])
    
    def get_tables_in_dataset(self, group_id, dataset_id):
        """Get list of tables in a dataset (useful for building DAX queries)."""
        headers = self._get_headers()
        
        # Use TMSL (Tabular Model Scripting Language) to get schema
        query_url = f"{self.BASE_URL}/groups/{group_id}/datasets/{dataset_id}/executeQueries"
        
        # Simple query to get table names
        dax_query = "EVALUATE INFO.TABLES()"
        
        query_request = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }
        
        response = requests.post(query_url, json=query_request, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            if 'results' in result and len(result['results']) > 0:
                tables = result['results'][0].get('tables', [])
                if tables:
                    return pd.DataFrame(tables[0].get('rows', []))
        
        return pd.DataFrame()


# Singleton instance
powerbi_service = PowerBIService()
