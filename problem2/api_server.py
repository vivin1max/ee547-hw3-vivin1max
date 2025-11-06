#!/usr/bin/env python3

import argparse
import json
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote

import boto3
from boto3.dynamodb.conditions import Key


class PaperAPIHandler(BaseHTTPRequestHandler):
    dynamodb = None
    table_name = None
    region = None
    
    def log_message(self, format, *args):
        sys.stdout.write(f"{self.address_string()} - [{self.log_date_time_string()}] {format % args}\n")
    
    def send_json_response(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str, indent=2).encode())
    
    def send_error_response(self, message, status=500):
        self.send_json_response({"error": message}, status)
    
    def do_GET(self):
        """Handle GET requests."""
        try:
            # Parse URL
            parsed = urlparse(self.path)
            path_parts = [p for p in parsed.path.split('/') if p]
            query_params = parse_qs(parsed.query)
            
            # Initialize DynamoDB table
            if not hasattr(self, '_table'):
                dynamodb = boto3.resource('dynamodb', region_name=self.region)
                self._table = dynamodb.Table(self.table_name)
            
            # Route requests
            if len(path_parts) >= 2 and path_parts[0] == 'papers':
                
                # GET /papers/recent?category={cat}&limit={n}
                if path_parts[1] == 'recent':
                    category = query_params.get('category', ['cs.LG'])[0]
                    limit = int(query_params.get('limit', ['20'])[0])
                    result = self.query_recent(category, limit)
                    self.send_json_response(result)
                
                # GET /papers/author/{author_name}
                elif path_parts[1] == 'author' and len(path_parts) >= 3:
                    author_name = unquote(' '.join(path_parts[2:]))
                    result = self.query_author(author_name)
                    self.send_json_response(result)
                
                # GET /papers/keyword/{keyword}?limit={n}
                elif path_parts[1] == 'keyword' and len(path_parts) >= 3:
                    keyword = path_parts[2]
                    limit = int(query_params.get('limit', ['20'])[0])
                    result = self.query_keyword(keyword, limit)
                    self.send_json_response(result)
                
                # GET /papers/search?category={cat}&start={date}&end={date}
                elif path_parts[1] == 'search':
                    category = query_params.get('category', ['cs.LG'])[0]
                    start = query_params.get('start', ['2020-01-01'])[0]
                    end = query_params.get('end', ['2030-12-31'])[0]
                    result = self.query_daterange(category, start, end)
                    self.send_json_response(result)
                
                # GET /papers/{arxiv_id}
                elif len(path_parts) == 2:
                    arxiv_id = path_parts[1]
                    result = self.get_paper(arxiv_id)
                    if result['papers']:
                        self.send_json_response(result)
                    else:
                        self.send_error_response("Paper not found", 404)
                
                else:
                    self.send_error_response("Invalid endpoint", 404)
            
            else:
                self.send_error_response("Invalid path", 404)
        
        except Exception as e:
            self.send_error_response(str(e), 500)
    
    def query_recent(self, category, limit):
        """Query recent papers in category."""
        start_time = time.time()
        response = self._table.query(
            KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
            ScanIndexForward=False,
            Limit=limit
        )
        return {
            "category": category,
            "papers": [{
                "arxiv_id": item['arxiv_id'],
                "title": item['title'],
                "authors": item['authors'],
                "published": item['published']
            } for item in response.get('Items', [])],
            "count": len(response.get('Items', [])),
            "execution_time_ms": round((time.time() - start_time) * 1000, 2)
        }
    
    def query_author(self, author_name):
        """Query papers by author."""
        start_time = time.time()
        response = self._table.query(
            IndexName='AuthorIndex',
            KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
        )
        return {
            "author": author_name,
            "papers": [{
                "arxiv_id": item['arxiv_id'],
                "title": item['title'],
                "authors": item['authors'],
                "published": item['published']
            } for item in response.get('Items', [])],
            "count": len(response.get('Items', [])),
            "execution_time_ms": round((time.time() - start_time) * 1000, 2)
        }
    
    def get_paper(self, arxiv_id):
        """Get paper by ID."""
        start_time = time.time()
        response = self._table.query(
            IndexName='PaperIdIndex',
            KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}'),
            Limit=1
        )
        items = response.get('Items', [])
        return {
            "papers": [{
                "arxiv_id": item['arxiv_id'],
                "title": item['title'],
                "authors": item['authors'],
                "published": item['published'],
                "categories": item.get('categories', []),
                "abstract": item.get('abstract', ''),
                "keywords": item.get('keywords', [])
            } for item in items],
            "count": len(items),
            "execution_time_ms": round((time.time() - start_time) * 1000, 2)
        }
    
    def query_daterange(self, category, start_date, end_date):
        """Query papers in date range."""
        start_time = time.time()
        response = self._table.query(
            KeyConditionExpression=(
                Key('PK').eq(f'CATEGORY#{category}') &
                Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
            )
        )
        return {
            "category": category,
            "date_range": {"start": start_date, "end": end_date},
            "papers": [{
                "arxiv_id": item['arxiv_id'],
                "title": item['title'],
                "authors": item['authors'],
                "published": item['published']
            } for item in response.get('Items', [])],
            "count": len(response.get('Items', [])),
            "execution_time_ms": round((time.time() - start_time) * 1000, 2)
        }
    
    def query_keyword(self, keyword, limit):
        """Query papers by keyword."""
        start_time = time.time()
        response = self._table.query(
            IndexName='KeywordIndex',
            KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
            ScanIndexForward=False,
            Limit=limit
        )
        return {
            "keyword": keyword,
            "papers": [{
                "arxiv_id": item['arxiv_id'],
                "title": item['title'],
                "authors": item['authors'],
                "published": item['published']
            } for item in response.get('Items', [])],
            "count": len(response.get('Items', [])),
            "execution_time_ms": round((time.time() - start_time) * 1000, 2)
        }


def main():
    parser = argparse.ArgumentParser(description="ArXiv Paper API Server")
    parser.add_argument("port", type=int, nargs='?', default=8080, help="Port to listen on")
    parser.add_argument("--table", default="arxiv-papers", help="DynamoDB table name")
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    args = parser.parse_args()
    
    # Set class variables
    PaperAPIHandler.table_name = args.table
    PaperAPIHandler.region = args.region
    
    # Start server
    server = HTTPServer(('', args.port), PaperAPIHandler)
    print(f"ArXiv Paper API Server running on port {args.port}")
    print(f"DynamoDB table: {args.table} (region: {args.region})")
    print(f"\nEndpoints:")
    print(f"  GET /papers/recent?category=cs.LG&limit=20")
    print(f"  GET /papers/author/<author_name>")
    print(f"  GET /papers/<arxiv_id>")
    print(f"  GET /papers/search?category=cs.LG&start=2020-01-01&end=2023-12-31")
    print(f"  GET /papers/keyword/<keyword>?limit=20")
    print(f"\nPress Ctrl+C to stop\n")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.shutdown()


if __name__ == "__main__":
    main()
