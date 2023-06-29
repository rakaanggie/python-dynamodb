import boto3
from botocore.exceptions import ClientError
from pprint import pprint
from decimal import Decimal
import time

client = boto3.client('dynamodb')

#create DynamoDB table
def create_movie_table():
    table = client.create_table(
        TableName = 'Movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH' #partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE' #sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

#Create record in a DynamoDB table
def put_movie(title, year, plot, rating):
    response = client.put_item(
        TableName = 'Movies',
        Item = {
            'year': {
                'N': "{}".format(year),
            },
            'title': {
                'S': "{}".format(title),
            },
            'plot': {
                "S": "{}".format(plot),
            },
            'rating': {
                "N": "{}".format(rating),
            },
        }
    )
    return response

#Get record fromm DynamoDB
def get_movie(title, year):
    try:
        response = client.get_item(
            TableName = 'Movies',
            Key={
                'year': {
                    'N': "{}".format(year),
                },
                'title': {
                    'S': "{}".format(title),
                }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

#Update a record in DynamoDb table
def update_movie(title, year, rating, plot, actors):
    response = client.update_item(
        TableName = 'Movies',
        Key = {
            'year': {
                'N': "{}".format(year),
            },
            'title': {
                'S': "{}".format(title),
            }
        },
        ExpressionAttributeNames = {
            '#R': 'rating',
            '#P': 'plot',
            '#A': 'actors'
        },
        ExpressionAttributeValues = {
            ':r': {
                'N': "{}".format(rating),
            },
            ':p': {
                'S': "{}".format(plot),
            },
            ':a': {
                'SS': actors,
            }
        },
        UpdateExpression = 'SET #R = :r, #P = :p, #A = :a',
        ReturnValues = "UPDATED_NEW"
    )
    return response

#Increment an atomic counter in DynamoDB table
def increase_rating(title, year, rating_increase):
    response = client.update_item(
        TableName = 'Movies',
        Key = {
            'year': {
                'N': "{}".format(year),
            },
            'title': {
                'S': "{}".format(title),
            }
        },
        ExpressionAttributeNames = {
            '#R': 'rating'
        },
        ExpressionAttributeValues = {
            ':r': {
                'N': "{}".format(Decimal(rating_increase)),
            }
        },
        UpdateExpression = 'SET #R = #R + :r',
        ReturnValues = "UPDATED_NEW"
    )
    return response

#Delete an item in DynamoDB table
def delete_underrated_movie(title, year, rating):
    try:
        response = client.delete_item(
            TableName = 'Movies',
            Key = {
                'year': {
                    'N': "{}".format(year),
                },
                'title': {
                    'S': "{}".format(title),
                }
            },
            ConditionExpression = "rating <= :a",
            ExpressionAttributeValues = {
                ':a': {
                    'N': "{}".format(rating),
                }
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response

if __name__ == '__main__':

    #  Create DynamoDB
    movie_table = create_movie_table()
    print("Create DynamoDB succeeded.........")
    print("Table status:{}".format(movie_table))
    time.sleep(30)

    #  Insert data into DynamoDB
    #movie_resp = put_movie("Just testing", 2023, "Test Movie..", 5)
    #print("Insert into DynamoDB suceeded..")
    #pprint(movie_resp, sort_dicts=False)

    #  Get an item from DynamoDB
    #movie = get_movie("Just testing", 2023)
    #if movie:
    #    print("Get an item from DynamoDb succeeded...")
    #    pprint(movie, sort_dicts=False)

    #  Update an item in DynamoDb
    #update_response = update_movie("Just testing", 2023, 5.5, "Testing Movie version 2....", ["Me", "Myself", "I"])
    #print("Update an item in DynamoDB succeeded...")
    #pprint(update_response, sort_dicts=False)

    # Increment an atomic counter in DynamoDB
    #update_response = increase_rating("Just testing", 2023, -1)
    #print("Increment an atomic counter in DYnamoDB succeeded...")
    #pprint(update_response, sort_dicts=False)

    # Delete an item in DynamoDB table
    #delete_response = delete_underrated_movie("Just testing", 2023, 7)
    #if delete_response:
    #    print("Deletean item in DynamoDB table....")
    #    pprint(delete_response, sort_dicts=False)