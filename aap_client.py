"""
Ansible Automation Platform API Client
Handles authentication and API requests to AAP
"""

import os
import httpx
from typing import Dict, List, Optional, Any
from pydantic import BaseModel
from dotenv import load_dotenv
import asyncio
import json
import yaml

load_dotenv()


class AAPConfig(BaseModel):
    """Configuration for AAP connection"""
    url: str
    token: str
    project_id: str
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


class JobTemplate(BaseModel):
    """Job Template model"""
    id: int
    name: str
    description: str
    project: int
    playbook: str
    inventory: Optional[int] = None
    credential: Optional[int] = None
    extra_vars: Optional[Dict[str, Any]] = None
    survey_enabled: bool = False


class JobLaunch(BaseModel):
    """Job Launch response model"""
    job: int
    ignored_fields: Dict[str, Any] = {}
    id: int
    type: str
    url: str
    related: Dict[str, str] = {}
    summary_fields: Dict[str, Any] = {}


class AAPClient:
    """Client for interacting with Ansible Automation Platform API"""
    
    def __init__(self, config: Optional[AAPConfig] = None):
        """Initialize AAP client with configuration"""
        if config:
            self.config = config
        else:
            # Load from environment variables
            self.config = AAPConfig(
                url=os.getenv("AAP_URL", ""),
                token=os.getenv("AAP_TOKEN", ""),
                project_id=os.getenv("AAP_PROJECT_ID", ""),
                verify_ssl=os.getenv("AAP_VERIFY_SSL", "True").lower() == "true",
                timeout=int(os.getenv("AAP_TIMEOUT", "30")),
                max_retries=int(os.getenv("AAP_MAX_RETRIES", "3"))
            )
        
        # Validate configuration
        if not self.config.url or not self.config.token:
            raise ValueError("AAP_URL and AAP_TOKEN must be configured")
        
        # Setup HTTP client with flexible auth
        headers = {"Content-Type": "application/json",
                   "Authorization": f"Bearer {self.config.token}"}
        
        self.client = httpx.AsyncClient(
            base_url=self.config.url,
            headers=headers,
            verify=self.config.verify_ssl,
            timeout=self.config.timeout
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with retry logic"""
        url = f"/api/controller/v2/{endpoint.lstrip('/')}"
        
        for attempt in range(self.config.max_retries):
            try:
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if attempt == self.config.max_retries - 1:
                    raise Exception(f"AAP API request failed: {e.response.status_code} - {e.response.text}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise Exception(f"AAP API request failed: {str(e)}")
                await asyncio.sleep(2 ** attempt)
    
    async def _make_text_request(self, method: str, endpoint: str, **kwargs) -> str:
        """Make HTTP request expecting text response with retry logic"""
        url = f"/api/controller/v2/{endpoint.lstrip('/')}"
        
        for attempt in range(self.config.max_retries):
            try:
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.text
            except httpx.HTTPStatusError as e:
                if attempt == self.config.max_retries - 1:
                    # Avoid potential JSON parsing in error message
                    error_text = f"HTTP {e.response.status_code}"
                    try:
                        error_text += f" - {e.response.text}"
                    except:
                        error_text += " - Unable to read response text"
                    raise Exception(f"AAP API request failed: {error_text}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise Exception(f"AAP API request failed: {str(e)}")
                await asyncio.sleep(2 ** attempt)
        
        # This should never be reached due to the exceptions above, but needed for type checking
        return ""
    
    async def get_job_templates(self, project_id: Optional[str] = None) -> List[JobTemplate]:
        """Get job templates from the configured project"""
        if not project_id:
            project_id = self.config.project_id
        
        params = {"project": project_id, "page_size": 200}
        response = await self._make_request("GET", "job_templates/", params=params)
        
        templates = []
        for template_data in response.get("results", []):
            # Handle empty string extra_vars from API
            if "extra_vars" in template_data and template_data["extra_vars"] == "":
                template_data["extra_vars"] = None
            if "extra_vars" in template_data and isinstance(template_data["extra_vars"], str):
                try:
                    template_data["extra_vars"] = json.loads(template_data["extra_vars"])
                except json.JSONDecodeError:
                    # Try it as yaml
                    template_data["extra_vars"] = yaml.safe_load(template_data["extra_vars"])
            templates.append(JobTemplate(**template_data))
        
        return templates
    
    async def get_job_template(self, template_id: int) -> JobTemplate:
        """Get a specific job template by ID"""
        response = await self._make_request("GET", f"job_templates/{template_id}/")
        return JobTemplate(**response)
    
    async def launch_job_template(self, template_id: int, extra_vars: Optional[Dict[str, Any]] = None,
                                 inventory: Optional[int] = None, credentials: Optional[List[int]] = None) -> JobLaunch:
        """Launch a job template with optional parameters"""
        payload = {}
        
        if extra_vars:
            payload["extra_vars"] = extra_vars
        
        if inventory:
            payload["inventory"] = inventory
        
        if credentials:
            payload["credentials"] = credentials
        
        response = await self._make_request("POST", f"job_templates/{template_id}/launch/", json=payload)
        return JobLaunch(**response)
    
    async def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """Get job status and details"""
        response = await self._make_request("GET", f"jobs/{job_id}/")
        return response
    
    async def get_job_stdout(self, job_id: int) -> str:
        """Get job stdout/logs"""
        return await self._make_text_request("GET", f"jobs/{job_id}/stdout/?format=txt")
    
    async def test_connection(self) -> bool:
        """Test AAP connection and authentication"""
        try:
            await self._make_request("GET", "me/")
            return True
        except Exception:
            return False 