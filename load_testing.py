import boto3
from datetime import datetime
import uuid

from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")


def load_data(table_name):
    before = datetime.utcnow()
    table = dynamodb.Table(table_name)
    with table.batch_writer() as writer:
        for i in range(1000000):
            ls = {'unique_id': str(uuid.uuid4()), 'bsin': str(i), 'timestamp': f"time now {i}", 'diff_key': f"file_{i}",
                  'market_id': f"sample id {i}", 'operation_type': "insert", 'source': "test"}
            writer.put_item(Item=ls)
            print(ls['unique_id'], ls['bsin'])
    after = datetime.utcnow()
    return after - before


def create_table(table_name):
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'unique_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'unique_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'market_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'bsin',
                'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': "query_index",
                'KeySchema': [
                    {
                        'AttributeName': 'bsin',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'market_id',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                },
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


def query_table(key1, key2, table_name):
    table = dynamodb.Table(table_name)
    before = datetime.utcnow()
    response = table.query(
        IndexName='query_index',
        KeyConditionExpression=Key('bsin').eq(key1) & Key('market_id').eq(key2),
    )
    after = datetime.utcnow()
    print(response['Items'])
    return after - before


def delete_table(table_name):
    table = dynamodb.Table(table_name)
    table.delete()


if __name__ == '__main__':
    tablename = "Testing_pq_engine_blacklist_log"

    print("Creating table...")
    log_table = create_table(table_name=tablename)
    print("Table status:", log_table.table_status)

    print("Loading data...")
    load_time = load_data(table_name=tablename)
    print("Total time taken for loading:", load_time)

    print("Query execution...")
    query_time = query_table("10000", "sample id 10000", table_name=tablename)
    print("Query execution time:", query_time)

    print("Deleting table...")
    delete_table(table_name=tablename)
    print("Table deleted.")

