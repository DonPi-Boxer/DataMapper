#!/bin/bash

# Use find to get all directories and files, then sort them in reverse order
# to ensure that items are renamed from the deepest to the shallowest
find . -depth | while read -r file ; do
    # Extract directory path and base name
    dir=$(dirname "$file")
    base=$(basename "$file")

    # Replace spaces with underscores in the base name
    new_base=$(echo "$base" | tr ' ' '_')

    # Only proceed if a renaming is necessary
    if [ "$base" != "$new_base" ]; then
        # Construct new full path and rename
        new_file="$dir/$new_base"
        mv -T "$file" "$new_file"
    fi
done
