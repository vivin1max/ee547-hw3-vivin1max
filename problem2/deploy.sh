#!/bin/bash

if [ $# -lt 2 ] || [ $# -gt 5 ]; then
    echo "Usage: $0 <key_file> <ec2_host> [table_name] [region] [port]"
    exit 1
fi

KEY_FILE="$1"
EC2_HOST="$2"
TABLE_NAME="${3:-ArxivPapers}"
AWS_REGION="${4:-us-west-2}"
PORT="${5:-8080}"

echo "Deploying to $EC2_HOST (table=$TABLE_NAME, region=$AWS_REGION, port=$PORT)"

echo "Copying files..."
scp -i "$KEY_FILE" -o StrictHostKeyChecking=no api_server.py ec2-user@"$EC2_HOST":~
scp -i "$KEY_FILE" -o StrictHostKeyChecking=no requirements.txt ec2-user@"$EC2_HOST":~

echo "Installing and starting server..."
ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ec2-user@"$EC2_HOST" << EOF
  set -e
  if ! command -v python3 >/dev/null 2>&1; then
    sudo yum install -y python3 python3-pip
  fi

  pip3 install --user -r requirements.txt
  pkill -f "api_server.py" || true

  echo "Starting server on port $PORT..."
  nohup python3 api_server.py $PORT --table $TABLE_NAME --region $AWS_REGION > server.log 2>&1 &

  sleep 2
  tail -20 server.log || true
EOF

echo "Done! Test: curl \"http://$EC2_HOST:$PORT/papers/recent?category=cs.LG&limit=5\""
