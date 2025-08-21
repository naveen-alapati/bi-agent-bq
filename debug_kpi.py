#!/usr/bin/env python3
"""
Debug script for KPI generation to identify the 500 error.
Run this script to test the KPI service locally.
"""

import os
import sys
import json
from typing import List

# Add the backend/app directory to the path
sys.path.insert(0, 'backend/app')

from models import TableRef
from kpi import KPIService
from bq import BigQueryService
from embeddings import EmbeddingService, EmbeddingMode

def test_kpi_generation():
    """Test the KPI generation to identify the issue."""
    
    print("=== KPI Debug Test ===")
    
    # Check environment variables
    print("\n1. Environment Variables:")
    env_vars = [
        'PROJECT_ID', 'BQ_LOCATION', 'BQ_EMBEDDINGS_DATASET',
        'LLM_PROVIDER', 'GEMINI_API_KEY', 'GEMINI_MODEL'
    ]
    
    for var in env_vars:
        value = os.getenv(var)
        if value:
            if 'KEY' in var:
                print(f"  {var}: {'*' * min(len(value), 8)}... (length: {len(value)})")
            else:
                print(f"  {var}: {value}")
        else:
            print(f"  {var}: NOT SET")
    
    # Test LLM client initialization
    print("\n2. Testing LLM Client:")
    try:
        from llm import LLMClient
        llm = LLMClient()
        print(f"  LLM Provider: {llm.provider}")
        print(f"  Gemini Model: {llm.gemini_model}")
        print(f"  Gemini API Key: {'Set' if llm.gemini_api_key else 'NOT SET'}")
        
        # Test diagnostics
        print("\n3. Testing LLM Diagnostics:")
        diag = llm.diagnostics()
        print(f"  Diagnostics: {json.dumps(diag, indent=2)}")
        
    except Exception as e:
        print(f"  LLM Client Error: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test KPI service
    print("\n4. Testing KPI Service:")
    try:
        project_id = os.getenv('PROJECT_ID', 'test-project')
        location = os.getenv('BQ_LOCATION', 'US')
        dataset = os.getenv('BQ_EMBEDDINGS_DATASET', 'analytics_poc')
        
        # Mock services for testing
        class MockBigQueryService:
            def get_table_schema(self, dataset_id, table_id):
                return [{'name': 'date_col', 'type': 'DATE'}]
        
        class MockEmbeddingService:
            def build_table_summary_content(self, project_id, dataset_id, table_id, schema, samples):
                return "Mock table summary content"
        
        bq_service = MockBigQueryService()
        embedding_service = MockEmbeddingService()
        
        kpi_service = KPIService(
            bq=bq_service,
            embeddings=embedding_service,
            project_id=project_id,
            embedding_dataset=dataset,
            create_index_threshold=5000
        )
        
        print(f"  KPI Service initialized successfully")
        
        # Test custom KPI generation
        print("\n5. Testing Custom KPI Generation:")
        tables = [TableRef(projectId=project_id, datasetId='test_dataset', tableId='test_table')]
        description = "Test KPI for debugging"
        
        try:
            result = kpi_service.generate_custom_kpi(tables, description)
            print(f"  KPI Generation Result: {type(result)}")
            if isinstance(result, dict):
                print(f"  Result keys: {list(result.keys())}")
            else:
                print(f"  Result: {result}")
        except Exception as e:
            print(f"  KPI Generation Error: {e}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f"  KPI Service Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_kpi_generation()