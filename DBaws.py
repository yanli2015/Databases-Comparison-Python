from pymongo import MongoClient
import time
import pymysql.cursors
import json
import boto3
import decimal
import numpy
import sys

current_milli_time = lambda: int(round(time.time() * 1000))

def binomial_dist_gen(num_samples):
    n, p = 100, 0.5
    array = numpy.random.binomial(n, 0.5, num_samples)
    return array


def mongodb_write_throughput(num_samples):
    try:
        connection = MongoClient("ec2-54-226-210-49.compute-1.amazonaws.com:27017")
        db = connection.students.ctec121
        samples = binomial_dist_gen(num_samples)
        before = current_milli_time()
        for s in samples:
            student_name = "student" + str(s)
            student_grade = int(s)
            student_record = {'name': student_name, 'grade': student_grade}
            db.insert(student_record)
        after = current_milli_time()
        throughput = num_samples * 1000 / (after - before)
        return throughput
    except Exception as e:
        print(str(e))
    finally:
        connection.close()


def mongodb_read_throughput(num_samples):
    try:
        connection = MongoClient("ec2-54-226-210-49.compute-1.amazonaws.com:27017")
        db = connection.students.ctec121
        before = current_milli_time()
        results = db.find(limit=num_samples)
        for res in results:
            pass
        after = current_milli_time()
        throughput = num_samples * 1000 / (after - before)
        return throughput
    except Exception as e:
        print(str(e))
    finally:
        connection.close()


def dynamoDB_create_table():
    endpoint_url = "http://ec2-54-226-210-49.compute-1.amazonaws.com:8000"
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1c', endpoint_url=endpoint_url)
    table = dynamodb.create_table(
        TableName='Throughput_Test2',
        KeySchema=[
            {
                'AttributeName': 'student_name',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'student_grade',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'student_name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'student_grade',
                'AttributeType': 'N'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )


def dynamoDB_write_throughput(num_samples):
    try:
        endpoint_url = "http://ec2-54-226-210-49.compute-1.amazonaws.com:8000"
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1c', endpoint_url=endpoint_url)
        table = dynamodb.Table('Throughput_Test2')
        samples = binomial_dist_gen(num_samples)
        before = current_milli_time()
        for s in samples:
            student_name = "student" + str(s)
            student_grade = int(s);
            student_record = {'student_name': student_name, 'student_grade': student_grade}
            table.put_item(
                Item=student_record
            )
        after = current_milli_time()
        throughput = num_samples * 1000 / (after - before)
        return throughput
    except Exception as e:
        print(str(e))

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def dynamoDB_read_throughput(num_samples):
    endpoint_url = "http://ec2-54-226-210-49.compute-a1.amazonaws.com:8000"
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1c', endpoint_url=endpoint_url)
    table = dynamodb.Table('Throughput_Test2')
    index = 0
    before = current_milli_time()
    response = table.scan()
    for i in response['Items']:
        index += 1
        # print(json.dumps(i, cls=DecimalEncoder))
        if index == num_samples:
            after = current_milli_time()
            throughput = num_samples * 1000 / (after - before)
            return throughput

    while index < num_samples and 'LastEvaluatedKey' in response:
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        for i in response['Items']:
            index += 1
            # print(json.dumps(i, cls=DecimalEncoder))
            if index == num_samples:
                after = current_milli_time()
                throughput = num_samples * 1000 / (after - before)
                return throughput
    after = current_milli_time()
    throughput = num_samples * 1000 / (after - before)
    return throughput

def get_results_from_DB():
    endpoint_url = "http://ec2-54-226-210-49.compute-1.amazonaws.com:8000"
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1c', endpoint_url=endpoint_url)
    table = dynamodb.Table('Throughput_Test2e')
    data_list = []
    response = table.scan(
        # FilterExpression=fe,
        # ProjectionExpression=pe,
        # ExpressionAttributeNames=ean
        )

    for i in response['Items']:
        data = json.dumps(i, cls=DecimalEncoder)
        data_list.append(json.loads(data))
        print(data_list[0]["TimeStamp"])
        print(data_list)

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            # ProjectionExpression=pe,
            # FilterExpression=fe,
            # ExpressionAttributeNames= ean,
            ExclusiveStartKey=response['LastEvaluatedKey']
            )
        for i in response['Items']:
            data = json.dumps(i, cls=DecimalEncoder)
            data_list.append(json.loads(data))
        print(data_list.length())
    return data_list

def clean_table():
    endpoint_url = "http://ec2-54-226-210-49.compute-1.amazonaws.com:8000"
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1c', endpoint_url=endpoint_url)
    table = dynamodb.Table('Throughput_Test2')
    table.delete()
    dynamoDB_create_table()

def mysql_write_throughput(num_samples):
    try:
        connection = pymysql.connect(host='192.168.1.8',
                                     user='root',
                                     password='root',
                                     db='throughput_test',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            sql = "INSERT INTO 'test' (`student_name`, `student_grade`) VALUES (%s, %s)"
            samples = binomial_dist_gen(num_samples)
            before = current_milli_time()
            for s in samples:
                student_name = "student" + str(s)
                student_grade = int(s);
                cursor.execute(sql, (student_name, student_grade))
                # connection.commit()
            connection.commit()
            after = current_milli_time()
            throughput = num_samples * 1000 / (after - before)
            return throughput
    except Exception as e:
        print(str(e))
    finally:
        connection.close()


def mysql_read_throughput(num_samples):
    try:
        connection = pymysql.connect(host='192.168.0.8',
                                     user='root',
                                     password='root',
                                     db='throughput_test',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            sql = "SELECT '*' FROM `test` limit " + str(num_samples);
            cursor.execute(sql)
            before = current_milli_time()
            while True:
                result = cursor.fetchone()
                if result:
                    # print(result)
                    pass
                else:
                    break
            after = current_milli_time()
            throughput = num_samples * 1000 / (after - before)
            return throughput
    except Exception as e:
        print(str(e))
    finally:
        connection.close()


if __name__ == "__main__":
    dynamoDB_create_table()
    # get_results_from_DB()
    # dynamoDB_create_table()
    # mongodb_write_throughput(10)
