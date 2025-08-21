# KPI 500 Error Fix

## Problem Summary
The `/api/generate_custom_kpi` endpoint was returning a 500 error due to several configuration and code issues:

1. **Missing Environment Variables**: The `cloudrun.yaml` was missing critical environment variables needed for the KPI feature
2. **Missing projectId Field**: The `TableRef` model was missing the `projectId` field that the KPI service required
3. **Frontend-Backend Mismatch**: Frontend was sending tables without `projectId`, but backend expected it
4. **Insufficient Error Logging**: Limited logging made debugging difficult

## Root Causes

### 1. Environment Variables Missing
The deployment was setting environment variables via GitHub Actions but the `cloudrun.yaml` file was missing:
- `LLM_PROVIDER=gemini`
- `GEMINI_MODEL=gemini-2.0-flash`
- `GEMINI_API_KEY` (secret reference)

### 2. Data Model Mismatch
- **Frontend**: Sends `{datasetId, tableId}` 
- **Backend**: Expected `{projectId, datasetId, tableId}`
- **Result**: AttributeError when accessing `tables[0].projectId`

### 3. LLM Configuration Issues
- Gemini API key not properly configured
- Insufficient error handling in LLM client
- No fallback mechanisms for API failures

## Fixes Applied

### 1. Updated `cloudrun.yaml`
```yaml
env:
  - name: LLM_PROVIDER
    value: gemini
  - name: GEMINI_MODEL
    value: gemini-2.0-flash
  - name: GEMINI_API_KEY
    valueFrom:
      secretKeyRef:
        name: gemini-api-key
        key: latest
```

### 2. Fixed `TableRef` Model
```python
class TableRef(BaseModel):
    projectId: str  # Added missing field
    datasetId: str
    tableId: str
```

### 3. Added Backward Compatibility
Updated all KPI-related endpoints to handle frontend table format:
```python
# Convert frontend table format to backend TableRef format
tables = []
for table_data in tables_data:
    project_id = table_data.get('projectId', PROJECT_ID)
    dataset_id = table_data.get('datasetId')
    table_id = table_data.get('tableId')
    
    tables.append(TableRef(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id
    ))
```

### 4. Enhanced Error Logging
Added comprehensive logging throughout the KPI generation pipeline:
- LLM client initialization
- API call details
- Response parsing
- Fallback mechanisms

### 5. Improved Error Handling
- Better exception handling with stack traces
- Fallback KPI generation when LLM fails
- Graceful degradation for missing data

## Files Modified

1. **`cloudrun.yaml`** - Added missing environment variables
2. **`gemini-secret.yaml`** - Created Kubernetes secret template
3. **`backend/app/models.py`** - Added projectId to TableRef
4. **`backend/app/main.py`** - Added backward compatibility and error handling
5. **`backend/app/kpi.py`** - Enhanced error handling and logging
6. **`backend/app/llm.py`** - Added comprehensive logging
7. **`backend/app/diagnostics.py`** - Fixed TableRef instantiation
8. **`debug_kpi.py`** - Created debugging script

## Deployment Steps

### 1. Create Kubernetes Secret
```bash
# Encode your Gemini API key
echo -n "your-actual-api-key" | base64

# Update gemini-secret.yaml with the encoded value
# Apply the secret
kubectl apply -f gemini-secret.yaml
```

### 2. Update Cloud Run Service
```bash
# Deploy with updated configuration
gcloud run deploy analytics-kpi-poc \
  --image $IMAGE \
  --region $REGION \
  --allow-unauthenticated \
  --quiet \
  --set-env-vars PROJECT_ID=$PROJECT_ID,BQ_LOCATION=$REGION,BQ_EMBEDDINGS_DATASET=analytics_poc,LLM_PROVIDER=gemini,GEMINI_MODEL=gemini-2.0-flash \
  --set-secrets GEMINI_API_KEY=gemini-api-key:latest
```

### 3. Verify Deployment
```bash
# Check environment variables
gcloud run services describe analytics-kpi-poc --region=$REGION --format="value(spec.template.spec.containers[0].env[].name,spec.template.spec.containers[0].env[].value)"

# Test the endpoint
curl -X POST https://your-service-url/api/generate_custom_kpi \
  -H "Content-Type: application/json" \
  -d '{"tables":[{"datasetId":"test","tableId":"test"}],"description":"test"}'
```

## Testing

### Local Testing
```bash
# Set environment variables
export PROJECT_ID="your-project-id"
export LLM_PROVIDER="gemini"
export GEMINI_API_KEY="your-api-key"
export GEMINI_MODEL="gemini-2.0-flash"

# Run debug script
python debug_kpi.py
```

### Production Testing
1. Use the frontend to create a custom KPI
2. Check Cloud Run logs for any remaining errors
3. Verify the KPI is generated and displayed correctly

## Monitoring

### Key Metrics to Watch
- API response times
- Error rates by endpoint
- LLM API call success rates
- Memory and CPU usage

### Log Analysis
```bash
# View recent logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=analytics-kpi-poc" --limit=50

# Filter for errors
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=analytics-kpi-poc AND severity>=ERROR" --limit=20
```

## Future Improvements

1. **Frontend Updates**: Update frontend to include projectId in table objects
2. **Configuration Management**: Use ConfigMaps for non-sensitive configuration
3. **Health Checks**: Add health check endpoint for LLM services
4. **Rate Limiting**: Implement rate limiting for LLM API calls
5. **Caching**: Add caching for generated KPIs to reduce API calls

## Troubleshooting

### Common Issues

1. **"GEMINI_API_KEY must be set"**
   - Verify the secret is created and mounted
   - Check secret name and key in cloudrun.yaml

2. **"Invalid table data format"**
   - Ensure frontend sends proper table structure
   - Check that datasetId and tableId are present

3. **"Gemini API error 400"**
   - Verify API key is valid
   - Check request format and model name

4. **"No KPI data generated"**
   - Check LLM provider configuration
   - Verify API quotas and limits

### Debug Commands
```bash
# Check service configuration
gcloud run services describe analytics-kpi-poc --region=$REGION

# View service logs
gcloud logs tail --service=analytics-kpi-poc --region=$REGION

# Test LLM connectivity
curl -X POST https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=YOUR_KEY \
  -H "Content-Type: application/json" \
  -d '{"contents":[{"role":"user","parts":[{"text":"test"}]}]}'
```