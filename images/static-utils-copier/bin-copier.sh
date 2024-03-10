#!/bin/sh

# Copyright 2024 Flant JSC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export SOURCE_DIR=$1
export DEST_DIR=$2

if [ -z "$SOURCE_DIR" ]; then
  echo "Source directory is not specified"
  echo "Usage: $0 <source-dir> <dest-dir>"
  exit 1
fi

if [ ! -d "$SOURCE_DIR" ]; then
  echo "Source directory $SOURCE_DIR does not exist"
  exit 1
fi

if [ -z "$DEST_DIR" ]; then
  echo "Destination directory is not specified"
  echo "Usage: $0 <source-dir> <dest-dir>"
  exit 1
fi

if [ ! -d "$DEST_DIR" ]; then
  echo "Destination directory $DEST_DIR does not exist"
  exit 1
fi

cp -r "$SOURCE_DIR"/* "$DEST_DIR"
if [ $? -ne 0 ]; then
  echo "Failed to copy files from $SOURCE_DIR to $DEST_DIR"
  exit 1
fi

echo "Finished copying files from $SOURCE_DIR to $DEST_DIR"
