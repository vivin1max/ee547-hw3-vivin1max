#!/usr/bin/env python3

import argparse
import json
import sys
import time

import boto3
from boto3.dynamodb.conditions import Key


def parse_args():
    p = argparse.ArgumentParser(description="Query ArXiv papers in DynamoDB")
    p.add_argument("command", choices=["recent", "author", "get", "daterange", "keyword"])
    p.add_argument("args", nargs="+")
    p.add_argument("--table", default="arxiv-papers")
    p.add_argument("--region", default="us-west-2")
    p.add_argument("--limit", type=int, default=20)
    return p.parse_args()


def query_recent_in_category(table, category, limit=20):
    start_time = time.time()
    
    response = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=limit
    )
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    return {
        "query_type": "recent_in_category",
        "parameters": {"category": category, "limit": limit},
        "results": [{
            "arxiv_id": item['arxiv_id'],
            "title": item['title'],
            "authors": item['authors'],
            "published": item['published'],
            "categories": item['categories']
        } for item in response.get('Items', [])],
        "count": len(response.get('Items', [])),
        "execution_time_ms": round(execution_time_ms, 2)
    }


def query_papers_by_author(table, author_name):
    start_time = time.time()
    
    response = table.query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    return {
        "query_type": "papers_by_author",
        "parameters": {"author_name": author_name},
        "results": [{
            "arxiv_id": item['arxiv_id'],
            "title": item['title'],
            "authors": item['authors'],
            "published": item['published'],
            "categories": item['categories']
        } for item in response.get('Items', [])],
        "count": len(response.get('Items', [])),
        "execution_time_ms": round(execution_time_ms, 2)
    }


def get_paper_by_id(table, arxiv_id):
    start_time = time.time()
    
    response = table.query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}'),
        Limit=1
    )
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    items = response.get('Items', [])
    result = items[0] if items else None
    
    return {
        "query_type": "get_paper_by_id",
        "parameters": {"arxiv_id": arxiv_id},
        "results": [{
            "arxiv_id": result['arxiv_id'],
            "title": result['title'],
            "authors": result['authors'],
            "published": result['published'],
            "categories": result['categories'],
            "abstract": result.get('abstract', ''),
            "keywords": result.get('keywords', [])
        }] if result else [],
        "count": 1 if result else 0,
        "execution_time_ms": round(execution_time_ms, 2)
    }


def query_papers_in_date_range(table, category, start_date, end_date):
    start_time = time.time()
    
    response = table.query(
        KeyConditionExpression=(
            Key('PK').eq(f'CATEGORY#{category}') &
            Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
        )
    )
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    return {
        "query_type": "papers_in_date_range",
        "parameters": {"category": category, "start_date": start_date, "end_date": end_date},
        "results": [{
            "arxiv_id": item['arxiv_id'],
            "title": item['title'],
            "authors": item['authors'],
            "published": item['published'],
            "categories": item['categories']
        } for item in response.get('Items', [])],
        "count": len(response.get('Items', [])),
        "execution_time_ms": round(execution_time_ms, 2)
    }


def query_papers_by_keyword(table, keyword, limit=20):
    start_time = time.time()
    
    response = table.query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=limit
    )
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    return {
        "query_type": "papers_by_keyword",
        "parameters": {"keyword": keyword, "limit": limit},
        "results": [{
            "arxiv_id": item['arxiv_id'],
            "title": item['title'],
            "authors": item['authors'],
            "published": item['published'],
            "categories": item['categories']
        } for item in response.get('Items', [])],
        "count": len(response.get('Items', [])),
        "execution_time_ms": round(execution_time_ms, 2)
    }


def main():
    args = parse_args()
    
    # Initialize DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    table = dynamodb.Table(args.table)
    
    # Execute query based on command
    try:
        if args.command == "recent":
            if len(args.args) < 1:
                print("Usage: query_papers.py recent <category> [--limit N]", file=sys.stderr)
                sys.exit(1)
            result = query_recent_in_category(table, args.args[0], args.limit)
        
        elif args.command == "author":
            if len(args.args) < 1:
                print("Usage: query_papers.py author <author_name>", file=sys.stderr)
                sys.exit(1)
            result = query_papers_by_author(table, args.args[0])
        
        elif args.command == "get":
            if len(args.args) < 1:
                print("Usage: query_papers.py get <arxiv_id>", file=sys.stderr)
                sys.exit(1)
            result = get_paper_by_id(table, args.args[0])
        
        elif args.command == "daterange":
            if len(args.args) < 3:
                print("Usage: query_papers.py daterange <category> <start_date> <end_date>", file=sys.stderr)
                sys.exit(1)
            result = query_papers_in_date_range(table, args.args[0], args.args[1], args.args[2])
        
        elif args.command == "keyword":
            if len(args.args) < 1:
                print("Usage: query_papers.py keyword <keyword> [--limit N]", file=sys.stderr)
                sys.exit(1)
            result = query_papers_by_keyword(table, args.args[0], args.limit)
        
        else:
            print(f"Unknown command: {args.command}", file=sys.stderr)
            sys.exit(1)
        
        # Output JSON
        print(json.dumps(result, indent=2, default=str))
    
    except Exception as e:
        print(json.dumps({
            "error": str(e),
            "query_type": args.command
        }), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
