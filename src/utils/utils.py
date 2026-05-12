# Utility functions
import requests, aiohttp, json, os, gzip
from typing import Dict, List, Optional, Any, Union

def sendRequest_Sync(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    payload: Optional[Union[Dict[str, Any], str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = 30,
    allowRedirects: bool = True,
    verify: bool = True
) -> requests.Response:
    """
    Send an synchronous (blocking) HTTP request with support for all methods and payloads.
    
    Args:
        url: The target URL
        method: HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)
        headers: Optional headers dictionary
        payload: Request body (dict for JSON or string for raw data)
        params: URL query parameters
        timeout: Request timeout in seconds
        allowRedirects: Whether to follow redirects
        verify: Whether to verify SSL certificates
    
    Returns:
        Response object from requests library
    """
    # Check method validity
    method = method.upper()
    if method not in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    # Request kwargs
    _kwargs = {
        "headers": headers,
        "params": params,
        "timeout": timeout,
        "allow_redirects": allowRedirects,
        "verify": verify
    }
    
    # Handle payload
    if payload is not None:
        if isinstance(payload, dict):
            _kwargs["json"] = payload
        else:            
            _kwargs["data"] = payload
    
    # Send the request
    response = requests.request(method, url, **_kwargs)
    
    return response

async def sendRequest_Async(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    payload: Optional[Union[Dict[str, Any], str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = 30,
    allowRedirects: bool = True,
    verify: bool = True
) -> aiohttp.ClientResponse:
    """
    Send an async HTTP request with support for all methods and payloads.
    
    Args:
        url: The target URL
        method: HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)
        headers: Optional headers dictionary
        payload: Request body (dict for JSON or string for raw data)
        params: URL query parameters
        timeout: Request timeout in seconds
        allowRedirects: Whether to follow redirects
        verify: Whether to verify SSL certificates
    
    Returns:
        ClientResponse object from aiohttp
    """
    # Check method validity
    method = method.upper()
    if method not in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    timeout_obj = aiohttp.ClientTimeout(total=timeout) 
    connector = aiohttp.TCPConnector(ssl=verify)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        kwargs = {
            "headers": headers,
            "params": params,
            "timeout": timeout_obj,
            "allow_redirects": allowRedirects
        }
        
        if payload is not None:
            if isinstance(payload, dict):
                kwargs["json"] = payload
            else:
                kwargs["data"] = payload
        
        async with session.request(method, url, **kwargs) as response:
            # Read response body to keep it available after context exit
            await response.read()
            return response

def makeEmptyJSONLFile(filePath: str, compressed: bool = True) -> None:
    """
    Create an empty .jsonl or .jsonl.gz file if it doesn't exist.
    
    Args:
        filePath: Path to the file (add .gz extension for compressed)
        compressed: Whether to create a compressed file
    """
    if compressed and not filePath.endswith('.gz'):
        filePath += '.gz'
    
    os.makedirs(os.path.dirname(filePath), exist_ok=True)
    
    if not os.path.isfile(filePath):
        opener = gzip.open if compressed else open
        with opener(filePath, "wb" if compressed else "w", encoding=None if compressed else "utf-8"):
            pass

def readJSONL(filePath: str) -> List[Dict[str, Any]]:
    """
    Read a .jsonl or .jsonl.gz file and return a list of dictionaries.
    
    Args:
        filePath: Path to the file
    
    Returns:
        List of dictionaries read from the file
    """
    opener = gzip.open if filePath.endswith('.gz') else open
    mode = "rt" if filePath.endswith('.gz') else "r"
    
    with opener(filePath, mode, encoding="utf-8") as f:
        return [json.loads(line) for line in f]

def appendToJSONL(filePath: str, data: Union[list, Dict[str, Any]]) -> None:
    """
    Append data to a .jsonl or .jsonl.gz file.
    
    Args:
        filePath: Path to the file
        data: Dictionary or list of dictionaries to append
    """
    opener = gzip.open if filePath.endswith('.gz') else open
    mode = "at" if filePath.endswith('.gz') else "a"
    
    with opener(filePath, mode, encoding="utf-8") as f:
        if isinstance(data, dict):
            f.write(json.dumps(data) + "\n")
        elif isinstance(data, list):
            for item in data:
                f.write(json.dumps(item) + "\n")

def streamJsonlGz(filePath: str):
    """
    Stream a .jsonl.gz file line by line, yielding each line as a dictionary.
    
    Args:
        filePath: Path to the .jsonl.gz file
    """
    if not filePath.endswith('.gz'):
        raise ValueError("streamJsonlGz only supports .jsonl.gz files")
    
    with gzip.open(filePath, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def humanReadableFileSize(path):
    """
    Return the file size at `path` as a human-readable string (e.g. '3.2 MB').
    
    Args:
        path: Path to the file
    
    Returns:
        Human-readable file size
    """
    size = os.path.getsize(path)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"