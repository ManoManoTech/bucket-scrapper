#!/bin/bash

# Test parallel listing with a date range
echo "Testing parallel listing with date range..."

# List objects for a 7-day range (168 prefixes) 
./target/release/bucket-scrapper list \
    -b my-test-bucket \
    --start "2024-01-01T00:00:00Z" \
    --end "2024-01-07T23:59:59Z" \
    2>&1 | head -20

echo ""
echo "If you see 'Starting parallel listing of X prefixes' messages, parallel listing is working!"