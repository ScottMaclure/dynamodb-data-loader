"""
See 
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#batch-writing
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.batch_writer
"""

import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone
from string import Template

import boto3

dynamodb = boto3.resource('dynamodb')


def _get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="""
Bulk DynamoDB Templated Item Loader

Will read a json file as a template, substitute some supported $variables, then insert into dynamodb.

Steps:
1) Find out the name of your table you want to insert data into.
2) Create your-template-name.json. Use the examples provided for reference, note the "$" template vars available.
3) AWS_PROFILE=yourprofile python3 main.py -t your-table-name -f your-template-name.json -n 100
        """,
        formatter_class=argparse.RawTextHelpFormatter 
    )
    parser.add_argument('-f', '--file', help='File name', required=True)
    parser.add_argument('-t', '--table', help='Table name', required=True)
    parser.add_argument('-n', '--number', help='Number of items to insert', required=True)
    parser.add_argument('--commit', help='Write to dynamo', required=False, default=False, type=bool)
    args = parser.parse_args()
    return args


def _get_future_timestamp_seconds(minutes: int = 0) -> int:
    """
    Use settings to create a future timestamp (for expiry).
    Default is now (zero minutes into the future)
    """
    return int(
        (datetime.now(timezone.utc) + timedelta(minutes=minutes)).timestamp()
    )


def main(args) -> None:
    msg_prefix = "[DRY RUN] " if not args.commit else ""
    print(f'{msg_prefix}Batch creating {args.number} items using template {args.file} into table {args.table}...')
    
    table = dynamodb.Table(args.table)

    with open(args.file) as f:
        template = Template(f.read())

    with table.batch_writer() as batch:
        for i in range(1, int(args.number) + 1):
            if i % 1000 == 0:
                print(f'{msg_prefix}Batch putting item {i}')
            item = json.loads(template.substitute({
                # Modify these variables if you want to support other stuff in your templates. Up to you.
                'UUID' : str(uuid.uuid4()),
                'NOW': _get_future_timestamp_seconds(),
                'BUILD': i,
                'FOO': 'BAR'
            }))
            if args.commit:
                batch.put_item(Item=item)
            else:
                print(f'{msg_prefix}Templated item: {item}')
    
    print(f'{msg_prefix}Done.')


if __name__ == "__main__":
    main(_get_args())
