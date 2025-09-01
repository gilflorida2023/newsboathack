#!/bin/bash
#
OLLAMA_HOST=192.168.0.10:11434
# allmodels.sh - Ollama Model Testing Framework
#
# Description:
#   Tests all available Ollama models with a given article URL and records performance metrics.
#
# Usage:
#   ./allmodels.sh
#
# Features:
#   - Automatically tests all installed Ollama models
#   - Measures execution time for each model
#   - Logs results to output.txt
#   - Includes error handling and model cleanup
#
# Dependencies:
#   - ollama - For model management
#   - article2.sh - For article summarization
#
# Output Files:
#   - output.txt - Contains detailed test results
#
# Exit Codes:
#   0 - Success
#   1 - Error in model testing
#
# Examples:
#   ./allmodels.sh

# Function to validate OLLAMA_HOST parameter
validate_ollama_host() {
    if [ -z "$1" ]; then
        echo "Error: allmodels OLLAMA_HOST must be the first parameter and cannot be empty $2" >&2
        echo "Usage: func  <OLLAMA_HOST>" >&2
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
        echo "allmodels Warning: Failed to unload $unload_errors model(s)" >&2
        return 0
        #return 1
    else
        echo "allmodels All models unloaded successfully" >&2
        return 0
    fi
}

format_elapsed() {
    local seconds=$1
    local days=$((seconds / 86400))
    local hours=$((seconds % 86400 / 3600))
    local minutes=$((seconds % 3600 / 60))
    local secs=$((seconds % 60))

    if [ $days -gt 0 ]; then
        printf "%dd %dh %dm %ds" $days $hours $minutes $secs
    elif [ $hours -gt 0 ]; then
        printf "%dh %dm %ds" $hours $minutes $secs
    elif [ $minutes -gt 0 ]; then
        printf "%dm %ds" $minutes $secs
    else
        printf "%ds" $secs
    fi
}

ollama_list_models() {
    local host="$1"
    validate_ollama_host "$host" "ollama_list_models"

    curl -s http://${host}/api/tags | jq -r '.models[].name' | sort
}

# Define your skip list (space-separated)
# This is the list of models that dont follow my instructions.
# will perform aother run and compare what is left.
export SKIP_LIST=
#export SKIP_LIST="deepcoder:14b deepcoder:1.5b deepscaler:1.5b deepseek-r1:14b deepseek-r1:1.5b deepseek-r1:32b deepseek-r1:7b deepseek-r1:8b devstral:24b gemma3:1b gemma3:27b gemma3n:e2b gemma3n:e4b granite3.2-vision:2b granite3.2-vision-abliterated:2b granite3.3:2b granite3.3:2b-base granite3.3:8b granite3.3:8b-base huihui_ai/granite3.2-vision-abliterated:2b huihui_ai/magistral-abliterated:24b ibm/granite3.3:2b-base ibm/granite3.3:8b-base llava:7b magistral:24b magistral-abliterated:24bi minicpm-v:8b mistral-small3.1:24b mistral-small3.2:24b mistral-small3.2:24bi phi4-mini-reasoning:3.8b phi4-reasoning:latest qwen2.5:0.5b qwen2.5:1.5b qwen2.5:7b qwen3:30b qwen2.5:32b qwen2.5vl:32b qwen2.5vl:3b qwen2.5vl:7b qwen3:0.6b qwen3:14b qwen3:1.7b qwen3:32b qwen3:4b qwen3:8b qwq:latest"

ollama_list_edited() {
#    local host="$1"
#    validate_ollama_host "$host" "ollama_list_edited"
#    # Check if skip list is non-empty
#    if [[ -z "$SKIP_LIST" ]]; then
#        ollama_list_models $host
#    else
#        # Convert space-separated list to grep pattern
#        pattern=$(echo "$SKIP_LIST" | tr ' ' '|')
#        ollama_list_models $host| grep -vE "$pattern"
#    fi
    for i in llama3.2-vision:11b llama3:8b llama3.1:8b llama3.2:3b llama3.2:1b
    do 
        echo "$i"
    done
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
        echo "Warning: Failed to unload $unload_errors model(s)" >&2
        return 1
    else
        echo "All models unloaded successfully" >&2
        return 0
    fi
}


# Clear previous output
unload_ollama_models $OLLAMA_HOST

URL="https://thehill.com/homenews/administration/5476821-live-updates-trump-rfk-jr-cdc-government-shutdown/"
rm -f output.txt
# Main testing loop
for model in $(ollama_list_edited $OLLAMA_HOST); do
    echo "---------------------------------" | tee -a output.txt
    echo "Testing model: $model" | tee -a output.txt
    
    stime=$(date '+%s')
    bash article3.sh "$OLLAMA_HOST" "$URL" "$model" 257 "REPEATED" | tee -a output.txt
    exit_code=$?
    etime=$(date '+%s')
    
    elapsed=$((etime - stime))
    formatted_time=$(format_elapsed $elapsed)
    
    echo "Execution time: $formatted_time" | tee -a output.txt
    
    # Check for errors
    if [ $exit_code -ne 0 ]; then
        echo "WARNING: Model $model failed with exit code $exit_code" | tee -a output.txt
    fi
    
    unload_ollama_models $OLLAMA_HOST 
done

echo -e "\nTesting complete. Results saved to:"
echo "  - output.txt"
