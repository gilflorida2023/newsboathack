#!/bin/bash

# article3.sh - Article Summarization Tool
#
# Description:
#   Fetches and summarizes web articles using Ollama language models via API with caching
#   Detects repeated content when cache is fresh
#
# Usage:
#   ./article3.sh <OLLAMA_HOST> <URL> [OLLAMA_MODEL] [SUMMARY_LENGTH] [REPEATED]
#
# Parameters:
#   OLLAMA_HOST   - Required first parameter, host:port for Ollama server (e.g., 192.168.0.8:11434)
#   URL           - URL of the article to summarize
#   OLLAMA_MODEL  - Optional, model to use (default: llama3:8b)
#   SUMMARY_LENGTH - Optional, character length for summary (default: 257)
#   REPEATED      - Optional, set to "REPEATED" to allow processing of repeated content (default: detect repeats)

# Get the script name dynamically for cache directory
SCRIPT_NAME=$(basename "$0" .sh)

# Function to validate OLLAMA_HOST parameter
validate_ollama_host() {
    if [ -z "$1" ]; then
        echo "Error: OLLAMA_HOST must be the first parameter and cannot be empty $2" >&2
        echo "Usage: $SCRIPT_NAME <OLLAMA_HOST> <URL> [OLLAMA_MODEL] [SUMMARY_LENGTH] [REPEATED]" >&2
        exit 1
    fi
}

# Function to unload all Ollama models from memory
unload_ollama_models() {
    local host="$1"
    validate_ollama_host "$host" "unload_ollama_models"

    # Fetch loaded models (silently)
    local loaded_models
    loaded_models=$(curl -s "http://$host/api/ps" | jq -r '.models[].name' 2>/dev/null)

    # Exit if no models loaded
    if [ -z "$loaded_models" ]; then
        echo "No models currently loaded." >&2
        return 0
    fi

    # Show loaded models if verbose
    echo "Found loaded models:" >&2
    echo "$loaded_models" | sed 's/^/  /' >&2

    # Process each model
    local unload_errors=0
    while read -r model; do
        echo -n "Unloading $model... " >&2

        # Unload request
        curl -s -X POST "http://$host/api/generate" \
            -H "Content-Type: application/json" \
            -d '{"model": "'"$model"'", "prompt": "", "keep_alive": 0}' >/dev/null

        # Verify unload
        if curl -s "http://$host/api/ps" | jq -e --arg model "$model" \
           'any(.models[]?.name; . == $model)' >/dev/null; then
            echo "❌" >&2
            ((unload_errors++))
        else
            echo "✅" >&2
        fi
    done <<< "$loaded_models"

    # Return status
    if [ $unload_errors -gt 0 ]; then
        echo "article3 Warning: Failed to unload $unload_errors model(s)" >&2
        return 0
    else
        echo "article3 All models unloaded successfully" >&2
        return 0
    fi
}

# Check for required arguments
if [ $# -lt 2 ]; then
    echo "Error: Missing required parameters" >&2
    echo "Usage: $SCRIPT_NAME <OLLAMA_HOST> <URL> [OLLAMA_MODEL] [SUMMARY_LENGTH] [REPEATED]" >&2
    exit 1
fi

# Configuration
OLLAMA_HOST="$1"
validate_ollama_host "$OLLAMA_HOST" "article3 main module"
URL="$2"
OLLAMA_MODEL="${3:-llama3:8b}"
SUMMARY_LENGTH="${4:-257}"
ALLOW_REPEATED="${5:-no}"

#SUMMARY_PROMPT="Respond ONLY with the ${SUMMARY_LENGTH}-character summary. No thinking output. Summary: $(cat <<'EOF'
#Provide a concise summary of this article in exactly ${SUMMARY_LENGTH} characters or less (including spaces). 
#Focus on key points and policy implications if any.
#EOF
#)"
SUMMARY_PROMPT="Respond ONLY with the ${SUMMARY_LENGTH}-character summary. No thinking output. Summary: $(cat <<'EOF'
Provide a **strictly ${SUMMARY_LENGTH}-character** summary of this article.  
Structure: [Main Event] - [Key Detail] - [Outcome].  
Example: "Israel approves new Gaza offensive amid ceasefire talks; military plans phased operations while US seeks deal."  
**Rules:**  
1. **EXACTLY ${SUMMARY_LENGTH} chars** (count precisely).  
2. **No incomplete words** (truncate mid-sentence if needed).  
3. **No sources, dates, or author names.**  
4. **If over limit, rewrite shorter.**  
EOF
)"

#SUMMARY_PROMPT="Respond ONLY with the ${SUMMARY_LENGTH}-character summary. No thinking output. Summary: $(cat <<'EOF'
#Provide a concise summary of this article in exactly ${SUMMARY_LENGTH} characters or less (including spaces).
#Highlight Elon Musk’s free-speech advocacy and business potential (Tesla, Starlink, X, Grok/xAI, SpaceX)  emphasizing their innovation and economic benefits. Focus on policy implications favoring his ventures.
#EOF
#)"

# Debug output
echo "Using host: $OLLAMA_HOST" >&2
echo "Using model: $OLLAMA_MODEL" >&2
echo "Summary length: $SUMMARY_LENGTH" >&2
echo "Allow repeated content: $ALLOW_REPEATED" >&2

# Verify Ollama connection
check_ollama_server() {
    local host="$1"
    validate_ollama_host "$host" "check_ollama_server"
    
    echo "Checking Ollama server status..." >&2
    if ! curl -s "http://$host/api/tags" >/dev/null; then
        echo "Error: Cannot connect to Ollama at $host" >&2
        echo "Verify the server is running and accessible" >&2
        exit 1
    fi
    echo "Ollama server connection successful" >&2
}

# Verify model is available
check_model_available() {
    local host="$1"
    local model="$2"
    validate_ollama_host "$host" "check_model_available"
    
    echo "Checking if model '$model' is available..." >&2
    if ! curl -s "http://$host/api/tags" | jq -e ".models[] | select(.name == \"$model\")" >/dev/null; then
        echo "Error: Model '$model' not found on server" >&2
        echo "Available models:" >&2
        curl -s "http://$host/api/tags" | jq -r '.models[].name' >&2
        exit 1
    fi
    echo "Model '$model' is available" >&2
}

check_ollama_server "$OLLAMA_HOST"
check_model_available "$OLLAMA_HOST" "$OLLAMA_MODEL"

# Cache setup
CACHE_DIR="${HOME}/.cache/${SCRIPT_NAME}"
mkdir -p "$CACHE_DIR"
URL_HASH=$(echo -n "$URL" | md5sum | awk '{print $1}')
CACHE_FILE="${CACHE_DIR}/${URL_HASH}.txt"
# Add this line right after the CACHE_FILE is defined:
echo "Cache file: $CACHE_FILE" >&2

# Check dependencies
for cmd in curl lynx jq stat; do
    command -v $cmd &> /dev/null || { echo "Missing dependency: $cmd" >&2; exit 1; }
done

# Check for repeated content
check_repeated_content() {
    if [ "$ALLOW_REPEATED" = "REPEATED" ]; then
        echo "Skipping repeat check (REPEATED flag set)" >&2
        return 1
    fi

    if [ -f "$CACHE_FILE" ]; then
        current_time=$(date +%s)
        if stat -f %m "$CACHE_FILE" >/dev/null 2>&1; then
            file_mtime=$(stat -f %m "$CACHE_FILE")
        else
            file_mtime=$(stat -c %Y "$CACHE_FILE")
        fi
        
        cache_age_hours=$(( (current_time - file_mtime) / 3600 ))
        
        if [ $cache_age_hours -lt 24 ]; then
            echo "REPEATED"
            return 0
        fi
    fi
    return 1
}

# Check for repeated content before processing
if repeated_result=$(check_repeated_content); then
    echo "$repeated_result"
    exit 0
fi

# Get article content with caching
if [ -f "$CACHE_FILE" ]; then
    current_time=$(date +%s)
    if stat -f %m "$CACHE_FILE" >/dev/null 2>&1; then
        file_mtime=$(stat -f %m "$CACHE_FILE")
    else
        file_mtime=$(stat -c %Y "$CACHE_FILE")
    fi
    
    cache_age_hours=$(( (current_time - file_mtime) / 3600 ))
    echo "Cache file age: ${cache_age_hours}h" >&2
    
    if [ $cache_age_hours -ge 24 ]; then
        echo "Refreshing expired cache..." >&2
        # Modern Chrome user agent string
        CHROME_UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ARTICLE_CONTENT=$(curl -s -L -A "$CHROME_UA" "$URL" | lynx -dump -stdin -nolist -width=80)
        echo "$ARTICLE_CONTENT" > "$CACHE_FILE"
    else
        echo "Using cached content" >&2
        ARTICLE_CONTENT=$(cat "$CACHE_FILE")
    fi
else
    echo "Fetching new content..." >&2
    # Modern Chrome user agent string
    CHROME_UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ARTICLE_CONTENT=$(curl -s -L -A "$CHROME_UA" "$URL" | lynx -dump -stdin -nolist -width=80)
    echo "$ARTICLE_CONTENT" > "$CACHE_FILE"
fi

# Clean thinking tags
clean_thinking() {
    sed -E '
        /<[Tt][Hh][Ii][Nn][Kk]>/I,/<\/[Tt][Hh][Ii][Nn][Kk]>/Id;
        /[Tt][Hh][Ii][Nn][Kk]\.\.\./I,/done thinking\./Id;
        /[Tt][Hh][Ii][Nn][Kk][Ii][Nn][Gg]\.\.\./I,/done thinking\./Id;
        /^[[:space:]]*(Thinking|THINKING)[[:space:]]*$/Id;
        /^[[:space:]]*$/d;
        s/^[[:space:]]+//;
        s/[[:space:]]+$//;
        ' | \
    tr -s ' '
}

# Generate summary via API
generate_summary() {
    local host="$1"
    local model="$2"
    local prompt="$3"
    local content="$4"
    validate_ollama_host "$host" "generate_summary"
    
    local full_prompt="${prompt}\n\n${content}"
    echo "Generating summary (this may take a moment)..." >&2
    
    # Format prompt as JSON safely
    local prompt_json=$(jq -n --arg prompt "$full_prompt" '$prompt')
    
    echo "Sending API request..." >&2
    local response=$(curl -s -X POST "http://$host/api/generate" \
        -H "Content-Type: application/json" \
        -d "$(jq -n --arg model "$model" --argjson prompt "$prompt_json" '{
            model: $model,
            prompt: $prompt,
            stream: false
        }')" 2>&1)
    
    if [ $? -ne 0 ]; then
        echo "API request failed" >&2
        echo "Error details: $response" >&2
        return 1
    fi
    
    if ! echo "$response" | jq -e '.response' >/dev/null; then
        echo "Invalid API response format" >&2
        echo "Raw response: $response" >&2
        return 1
    fi
    
    echo "$response" | jq -r '.response'
}

# Main summary generation
unload_ollama_models "$OLLAMA_HOST"
summary=$(generate_summary "$OLLAMA_HOST" "$OLLAMA_MODEL" "$SUMMARY_PROMPT" "$ARTICLE_CONTENT" | clean_thinking)

# Fallback if empty
if [[ -z "$summary" || "$summary" =~ ^[[:space:]]*$ ]]; then
    echo "Warning: Empty summary received, using fallback content" >&2
    summary="[Error: No summary generated]"
fi

# Validate length
length=$(echo -n "$summary" | wc -m)
echo "Generated summary length: $length/$SUMMARY_LENGTH" >&2

if [ $length -gt $SUMMARY_LENGTH ]; then
    echo "Warning: Summary exceeded ${SUMMARY_LENGTH} characters ($length)" >&2
fi

# Final output (to stdout)
echo "$summary"

unload_ollama_models "$OLLAMA_HOST"
