### HTTP Request URI and Method 

URI : `{container}:5300/health`

Method : `GET` 


### Health Check Success 
Success Status Code : `200`


Success Respose Body:
```
{
  "status":"healthy","message":"Service is healthy."
}
```

### Health Check Failure
Failure Status Code : `503`


Failure Respose Body:
```
{
  "status": "unhealthy",
  "message": "Service not available. Please try after some time."
}
```

