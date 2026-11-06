# TODO — remaining work

Work to accomplish.  Delete from this file when the change is ready to 
commit.

## probe mitigations

before even contacting upstream ollama, verify the path. If invalid, 
pretend to be nginx and return a 444. Below are three lightweight
strategies to employ:

### Block Known Probe Extensions & Patterns

Attackers look for file extensions or specific admin panels that Ollama will never use. You can catch 99% of casual scans with a quick prefix or suffix check:

BLOCKED_EXTENSIONS = ('.php', '.asp', '.aspx', '.jsp', '.env', '.yaml', '.yml', '.git')
BLOCKED_PATHS = ('/wp-admin', '/admin', '/cgi-bin', '/phpmyadmin', '/.well-known')

def is_obviously_a_probe(path: str) -> bool:
    path_lower = path.lower()
    
    # Check if they are looking for configuration files or scripts
    if path_lower.endswith(BLOCKED_EXTENSIONS):
        return True
        
    # Check if they are targeting common non-Ollama frameworks
    if path_lower.startswith(BLOCKED_PATHS):
        return True
        
    return False

### Implement a Max Path Length

Ollama paths are clean and predictable (e.g., /api/chat or /v1/chat/completions). Probes often contain massive, deeply nested directory traversal attempts or exploit payloads.
• Setting a strict maximum path length (e.g., under 100 characters) will automatically kill complex exploit attempts without breaking any legitimate Ollama API call.

### Verify prefix

except for our /status page, every path for ollama should start with /api or /v1

def is_valid_ollama(path: str) -> bool:
    # Allow the custom base route you created
    if path == "/status":
        return True
        
    # Ollama routes almost exclusively live under /api/ or /v1/
    # By matching the structural prefix, new endpoints like /api/foo will automatically work
    if path.startswith("/api/") or path.startswith("/v1/"):
        return True
        
    return False

### collapse all 404s records after 10 minutes

These are usually hack probes and just fill up memory. Collapse "request
history" and "requests by path" entries into a single line after, say,
10 minutes.
