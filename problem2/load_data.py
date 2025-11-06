#!/usr/bin/env python3

import argparse
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key

STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'can', 'this', 'that', 'these', 'those', 'we', 'our', 'use', 'using',
    'based', 'approach', 'method', 'paper', 'propose', 'proposed', 'show'
}


def parse_args():
    p = argparse.ArgumentParser(description="Load ArXiv papers into DynamoDB")
    p.add_argument("papers_json", help="Path to papers.json from HW1")
    p.add_argument("table_name", help="DynamoDB table name")
    p.add_argument("--region", default="us-west-2", help="AWS region")
    p.add_argument("--skip-create", action="store_true", help="Skip table creation")
    return p.parse_args()


def extract_keywords(abstract, top_n=10):
    if not abstract:
        return []
    words = re.findall(r'\b[a-z]{3,}\b', abstract.lower())
    filtered = [w for w in words if w not in STOPWORDS]
    counts = Counter(filtered)
    return [word for word, _ in counts.most_common(top_n)]


def create_table(dynamodb, table_name):
    print(f"Creating table: {table_name}")
    
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'PK', 'KeyType': 'HASH'},
                {'AttributeName': 'SK', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'PK', 'AttributeType': 'S'},
                {'AttributeName': 'SK', 'AttributeType': 'S'},
                {'AttributeName': 'GSI1PK', 'AttributeType': 'S'},
                {'AttributeName': 'GSI1SK', 'AttributeType': 'S'},
                {'AttributeName': 'GSI2PK', 'AttributeType': 'S'},
                {'AttributeName': 'GSI3PK', 'AttributeType': 'S'},
                {'AttributeName': 'GSI3SK', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'AuthorIndex',
                    'KeySchema': [
                        {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                        {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                },
                {
                    'IndexName': 'PaperIdIndex',
                    'KeySchema': [
                        {'AttributeName': 'GSI2PK', 'KeyType': 'HASH'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                },
                {
                    'IndexName': 'KeywordIndex',
                    'KeySchema': [
                        {'AttributeName': 'GSI3PK', 'KeyType': 'HASH'},
                        {'AttributeName': 'GSI3SK', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        
        print("Creating GSIs: AuthorIndex, PaperIdIndex, KeywordIndex")
        print("Waiting for table to become active...")
        table.wait_until_exists()
        time.sleep(10)
        print(f"Table {table_name} created")
        return table
        
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        print(f"Table {table_name} already exists")
        return dynamodb.Table(table_name)


def load_papers(papers_json_path):
    with open(papers_json_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_dynamodb_items(paper):
    items = []
    arxiv_id = paper.get('id', 'unknown')
    title = paper.get('title', '')
    authors = paper.get('authors', [])
    abstract = paper.get('summary', '')
    categories = paper.get('categories', [])
    published = paper.get('published', '')
    
    keywords = extract_keywords(abstract, top_n=10)
    
    base_data = {
        'arxiv_id': arxiv_id,
        'title': title,
        'authors': authors,
        'abstract': abstract,
        'categories': categories,
        'published': published,
        'keywords': keywords
    }
    
    for category in (categories if categories else ['uncategorized']):
        items.append({
            'PK': f'CATEGORY#{category}',
            'SK': f'{published}#{arxiv_id}',
            'GSI2PK': f'PAPER#{arxiv_id}',
            **base_data
        })
    
    for author in (authors if authors else ['Unknown']):
        items.append({
            'PK': f'AUTHOR#{author}',
            'SK': f'{published}#{arxiv_id}',
            'GSI1PK': f'AUTHOR#{author}',
            'GSI1SK': published,
            'GSI2PK': f'PAPER#{arxiv_id}',
            **base_data
        })
    
    for keyword in keywords:
        items.append({
            'PK': f'KEYWORD#{keyword}',
            'SK': f'{published}#{arxiv_id}',
            'GSI3PK': f'KEYWORD#{keyword}',
            'GSI3SK': published,
            'GSI2PK': f'PAPER#{arxiv_id}',
            **base_data
        })
    
    return items


def batch_write_items(table, items, batch_size=25):
    """Write items to DynamoDB in batches."""
    total = len(items)
    written = 0
    
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
            written += 1
            if written % 100 == 0:
                print(f"  Written {written}/{total} items...")
    
    return written


def main():
    args = parse_args()
    
    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    
    if not args.skip_create:
        table = create_table(dynamodb, args.table_name)
    else:
        table = dynamodb.Table(args.table_name)
    
    # Load papers
    print(f"Loading papers from {args.papers_json}...")
    papers = load_papers(args.papers_json)
    print(f"Loaded {len(papers)} papers from JSON")
    
    # Extract keywords and create items
    print("Extracting keywords from abstracts...")
    all_items = []
    category_count = 0
    author_count = 0
    keyword_count = 0
    
    for paper in papers:
        items = create_dynamodb_items(paper)
        all_items.extend(items)
        
        # Count by type
        categories = paper.get('categories', []) or ['uncategorized']
        authors = paper.get('authors', []) or ['Unknown']
        abstract = paper.get('summary', '')
        keywords = extract_keywords(abstract, top_n=10)
        
        category_count += len(categories)
        author_count += len(authors)
        keyword_count += len(keywords)
    
    # Batch write to DynamoDB
    print(f"Writing {len(all_items)} items to DynamoDB...")
    written = batch_write_items(table, all_items)
    
    # Report statistics
    print(f"\nLoaded {len(papers)} papers")
    print(f"Created {written} DynamoDB items (denormalized)")
    denorm_factor = written / len(papers) if papers else 0
    print(f"Denormalization factor: {denorm_factor:.1f}x")
    print(f"\nStorage breakdown:")
    print(f"  - Category items: {category_count} ({category_count/len(papers):.1f} per paper avg)")
    print(f"  - Author items: {author_count} ({author_count/len(papers):.1f} per paper avg)")
    print(f"  - Keyword items: {keyword_count} ({keyword_count/len(papers):.1f} per paper avg)")
    print(f"  - Total: {category_count + author_count + keyword_count}")


if __name__ == "__main__":
    main()
