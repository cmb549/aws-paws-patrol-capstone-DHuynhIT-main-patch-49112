import json
import boto3
import datetime
import time

#call the S3 bucket
s3_client = boto3.client('s3')

#call the sns service
client = boto3.client('sns')

#call the DB service
DBresource = boto3.resource('dynamodb')  
DBclient = boto3.client('dynamodb')


def lambda_handler(event, context):
    
    #Check if the dynamoDB has information or not
    
    grabTables = DBclient.list_tables()['TableNames']
    
    for findtable in grabTables:
        if findtable == "WatchDogDB2.0":
            setTable = DBresource.Table(findtable)
            setTable.delete()    
    
    #Set a delay to allow the deletion of the table
    time.sleep(60)
    
    
    #Create Table
    DBclient.create_table(
    TableName='WatchDogDB2.0',
    KeySchema=[
        {
            "AttributeName": "Dog ID","KeyType": "HASH"
        }
        ],
    AttributeDefinitions=[
        {
            "AttributeName": "Dog ID","AttributeType": "N"
        }
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10
            }
        )
    
    #Set a delay to allow the creation of the table
    time.sleep(60)    
  
    
    #Bucket Info
    bucket = 'watchdogstuff'
    AdoptFile = 'Adopted.json'
    AdoptableFile = 'AdoptableDogs.json'
    
    #Grab the Adopted and Adoptable file from S3
    Get_AdoptedList = s3_client.get_object(Bucket=bucket,Key=AdoptFile)
    Get_AdoptableList = s3_client.get_object(Bucket=bucket,Key=AdoptableFile)
    
    #Read the Adopted and Adoptable file from s3 
    Read_AdoptedList = Get_AdoptedList['Body'].read().decode("utf-8")
    Read_AdoptableList = Get_AdoptableList['Body'].read().decode("utf-8")
    
    #load the Adopted and Adoptable File content into a variable
    Load_AdoptedList = json.loads(Read_AdoptedList)
    Load_AdoptableList = json.loads(Read_AdoptableList)
   
   
    table = DBresource.Table('WatchDogDB2.0')
    #Move Adopted list to dynamoDB
    for oneRecord in Load_AdoptedList['animals']:
        table.put_item(
            Item={
                'Dog ID':oneRecord['id'],
                'Dog Breed':oneRecord['breeds']['primary'],
                'Age':oneRecord['age'],
                'Gender':oneRecord['gender'],
                'Size':oneRecord['size'],
                'Adoption Status':oneRecord['status']
                }
            )
         
    #Move Adoptable list to dynamoDB
    for oneRecord in Load_AdoptableList['animals']:
        table.put_item(
            Item={
                'Dog ID':oneRecord['id'],
                'Dog Breed':oneRecord['breeds']['primary'],
                'Age':oneRecord['age'],
                'Gender':oneRecord['gender'],
                'Size':oneRecord['size'],
                'Adoption Status':oneRecord['status']
                }
            )
    
    
    #Grab our time
    GetTime = datetime.datetime.now()
    timestamp = GetTime.strftime("%d/%m/%Y %H:%M:%S")
    
    #Grab our SNS information
    topic_arn = 'arn:aws:sns:us-east-1:537386496637:LambdaTriggerNotification'
    
    message = 'Your monthly report on Dogs that are adoptable and adopted was imported to the database : ' + timestamp
    
    #Send the email notification to the team
    client.publish(TopicArn=topic_arn,Message=message)
    