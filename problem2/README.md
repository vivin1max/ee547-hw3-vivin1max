# Problem 2: ArXiv Paper Discovery with DynamoDB

DynamoDB-based paper discovery system with denormalization and GSIs.

## Quick Start

```bash
# Load data
python load_data.py papers.json ArxivPapers --region us-west-2

# Query papers
python query_papers.py recent cs.LG --limit 10
python query_papers.py author "Geoffrey Hinton"

# Start API server
python api_server.py 8080 --table ArxivPapers --region us-west-2
```

## Schema Design Decisions

**Partition Key Structure:**
Used `CATEGORY#{category}`, `AUTHOR#{author}`, `KEYWORD#{keyword}` prefixes to enable different access patterns on the same table. Sort key `{date}#{id}` gives chronological ordering. This avoids scanning and keeps queries fast.

**GSIs (3 total):**
- **AuthorIndex (GSI1)**: Query all papers by specific author
- **PaperIdIndex (GSI2)**: Direct lookup by ArXiv ID
- **KeywordIndex (GSI3)**: Find papers by keyword

Created separate GSIs instead of overloading main table because DynamoDB only allows one partition key per query.

**Denormalization Trade-offs:**
Each paper becomes 4+ items (category copies + author copies + keyword copies). This wastes storage but eliminates joins and makes queries predictable. Write complexity increases but reads stay fast.

## Denormalization Analysis

**Test dataset (10 papers):**
- Average items per paper: 4.0
- Storage multiplication: 4.0x
- Breakdown: 20 category items + 20 author items + 0 keyword items (empty abstracts)

**Most duplication:** Author denormalization (~2 authors per paper means doubling storage just for author access).

## Query Limitations

**NOT efficiently supported:**
1. **Count papers by author** - Requires scanning entire AuthorIndex, no COUNT aggregation
2. **Most cited papers globally** - Would need citation count as GSI sort key, lots of updates
3. **Full-text search** - Keyword extraction is basic, no phrase/fuzzy matching
4. **Multi-category AND queries** - Can only query one category at a time

**Why difficult:** DynamoDB has no COUNT/SUM/AVG, no joins, limited filtering (still charged for scanned items), and no full-text search.

## When to Use DynamoDB

**Choose DynamoDB when:**
- Access patterns are known upfront and limited
- Need consistent low latency (<10ms) at massive scale
- Willing to trade storage for speed
- Want serverless (no DB management)

**Choose PostgreSQL when:**
- Need complex queries, JOINs, aggregations
- Ad-hoc analytics and reporting
- Storage cost matters
- Strong consistency and relational integrity required

**Trade-off:** DynamoDB scales horizontally but forces denormalization. PostgreSQL is flexible but harder to scale.

## EC2 Deployment

**Instance:** 52.15.129.32 (ec2-52-15-129-32.us-east-2.compute.amazonaws.com)  
**IAM Role:** arn:aws:iam::342671696681:role/ec2-dynamodb-read-role  
**Region setup:** EC2 in us-east-2, DynamoDB in us-west-2 (cross-region adds ~60ms latency)

**Challenges:**
- IAM permissions initially missing - needed DynamoDBFullAccess for table creation
- URL encoding broke author names with spaces - fixed with `unquote()`
- Cross-region required explicit `--region` parameter
- Empty abstracts in test data meant no keywords extracted (logic works fine though)


