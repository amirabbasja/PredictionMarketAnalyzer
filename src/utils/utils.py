# Utility functions
import requests, aiohttp, json, os, gzip
from typing import Dict, List, Optional, Any, Union
from enum import Enum

# Used for compatibility with python 3.11 and below, which don't have StrEnum
try:
    from enum import StrEnum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass

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

def countLines( filepath):
    """
    Counts lines in a jsonl or jsonl.gz file without loading it into memory.

    Args:
        filepath (str): The location of the file.

    Returns:
        int: The number of lines in the file.
    """
    opener = gzip.open if filepath.endswith('.gz') else open
    
    with opener(filepath, 'rb') as f:
        return sum(1 for _ in f)

def getLastNLines( filepath, n=1000):
    """
    Reads the last n lines from the file at the given filepath without reading it entirely.
    Helps when file is so large that we run out of memory
    """
    opener = gzip.open if filepath.endswith('.gz') else open
    
    with opener(filepath, 'rb') as f:
        # Get file size
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        
        # Start from end
        buffer_size = 8192
        lines_found = []
        block_end = file_size
        
        while len(lines_found) < n and block_end > 0:
            print("Seeking. Lines found: ", len(lines_found))
            # Calculate block start position
            block_start = max(0, block_end - buffer_size)
            f.seek(block_start)
            
            # Read chunk
            chunk = f.read(block_end - block_start)
            
            # Split into lines
            chunk_lines = chunk.split(b'\n')
            
            # Handle partial line at start (except first block)
            if block_start > 0 and chunk_lines:
                chunk_lines = chunk_lines[1:]
            
            # Add lines to beginning of list
            lines_found = chunk_lines + lines_found
            block_end = block_start
        
        # Get last n lines and decode
        last_n = lines_found[-n:] if len(lines_found) > n else lines_found
        return [line.decode('utf-8') for line in last_n if line]

def saveProgress(path: str, data: Dict[str, Any]) -> None:
    """
    Saves progress of fetching data to a .progress file.
    """
    # Get directory, handle edge case where path is just a filename (empty directory)
    directory = os.path.dirname(path)
    if not directory:
        directory = "."
        
    saveFile = os.path.join(directory, ".progress")
    saveKey = os.path.basename(path).split(".")[0]
    
    # Create directories if they don't exist
    os.makedirs(directory, exist_ok=True)
    
    # Initialize empty dictionary
    file_data = {}
    
    # Check if the .progress FILE exists (not just the directory)
    if os.path.exists(saveFile):
        try:
            with open(saveFile, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
        except json.JSONDecodeError:
            # Handle case where file exists but is empty or invalid JSON
            pass

    # Change/add the key with the file path to the new data
    file_data[saveKey] = data

    # Save it back to the file (this creates it if it didn't exist)
    with open(saveFile, 'w', encoding='utf-8') as f:
        json.dump(file_data, f, indent=4)

def loadABI(blockchain: str, contractName: str) -> Optional[Dict[str, Any]]:
    """
    Load the ABI for a given blockchain and contract name from the ABIs directory.
    The ABI files should have `.abi` extension
    
    Args:
        blockchain: The name of the blockchain (e.g. "polygon")
        contractName: The name of the contract (e.g. "exchange_CFT_v2", no extensions)
    
    Returns:
        The ABI as a list of dictionaries, or None if not found
    """
    # Process contract name
    contractName = contractName.replace(".abi", "").replace(".json", "")
    
    abiPath = os.path.join("src", "ABIs", blockchain, f"{contractName}.abi")
    
    if not os.path.isfile(abiPath):
        print(f"ABI file not found: {abiPath}")
        return None
    
    with open(abiPath, 'r', encoding='utf-8') as f:
        return json.load(f)

class Errors(StrEnum):
    MISSING_API_KEY = "UNACCEPTABLE_API_KEY"
    WRONG_ARGUMENTS = "WRONG_ARGUMENTS"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    REQUEST_ERROR = "REQUEST_ERROR"
    RATE_LIMITED = "RATE_LIMITED"