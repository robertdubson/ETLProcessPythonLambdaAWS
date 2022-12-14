import json
import uuid
import pandas as pd
import boto3
import numpy

s3 = boto3.client('s3')


def get_object_from_s3_bucket():
    bucket = 'finaltaskawsbucket'
    key = '100_best_books.json'
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body']

    jsonObj = json.loads(content.read())

    return jsonObj


def import_data_from_json(object):
    """
    this function loads data from JSON to either pandas DataFrame or a dict for further analysis
    :param object: a JSON object from S3 bucket
    :return: pandas DF or dict of data
    """
    book_list = []
    for record in object:
        book = [str(record['author']), str(record['country']), str(record['imageLink']), str(record['language']),
                str(record['link']),
                int(record['pages']), str(record['title']), str(record['year'])]
        book_list.append(book)

    df = pd.DataFrame(book_list,
                      columns=['author', 'country', 'imageLink', 'language', 'link', 'pages', 'title', 'year'])

    return df


def drop_unnecessary_columns(df, columns_to_drop: list):
    """
    this function drops unnecessary culumns from pandas DF or dict of data
    :param df: pandas DF or dict of data
    :param columns_to_drop: list of columns to drop from df of dict of data
    :return: dataframe of dict with only necessary data for further analysis
    """
    for col in columns_to_drop:
        df.drop(col, axis=1, inplace=True)

    return df


def reorder_columns_in_data(df, columns_order: list):
    """
    this function reorders data in dataframe or dict of data
    :param df: pandas DF or dict of data
    :param columns_order: list of columns with proper columns order
    :return: ordered dataframe or dict
    """
    for col in columns_order:
        if col not in df.columns:
            raise Exception("There is no such columns in current dataframe")
    df = df[columns_order]

    return df


def language_transformation(df, lang_dict: dict):
    """
    this function substitutes full language names with their code in ISO 639-1 format
    :param df: pandas DF or dict of data
    :param lang_dict: dictionary with language codes in ISO 639-1 format
    :return: pandas dataframe or dict of data with replaced languages
    """
    for ind in df.index:
        languages = []
        languages = str(df['language'][ind]).split(',')
        new_lang = []
        for lang in languages:
            new_lang.append(lang_dict[lang.strip()])
        fin_str = ""
        for lang in new_lang:
            fin_str += lang.strip()
            fin_str += ", "
        length = len(fin_str) - 2
        finest_str = ""
        for i in fin_str:
            if len(finest_str) != length:
                finest_str += i
        df['language'][ind] = finest_str

    return df


def year_transformation(df):
    """
    this is an OPTIONAL task to transform year to standard BC and AD format
    :param df: pandas DF or dict of data
    :return: dataframe or dict of data with replaced years
    """
    for ind in df.index:
        cur_year = int(df['year'][ind])
        if cur_year < 0:
            year_module = 0 - cur_year
            year_str = str(year_module) + " BC"
            df['year'][ind] = year_str
        else:
            year_module = cur_year
            year_str = str(year_module) + " AD"
            df['year'][ind] = year_str

    return df


def load_data_to_dynamo_db_table(df):
    """
    this function loads data to DynamoDB table from dataframe or dictionary
    :param df: dataframe or dict of data to load to DynamoDB table
    :return: status: e.g. 200 - Success
    """
    dynamodb = boto3.client('dynamodb')
    key_schema = [
        {
            'AttributeName': 'uuid',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'year',
            'KeyType': 'RANGE'
        }
    ]
    attribute_definitions = [
        {
            'AttributeName': 'uuid',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'year',
            'AttributeType': 'S'
        }
    ]

    table_name = 'best_books'

    existing_tables = dynamodb.list_tables()['TableNames']

    if table_name not in existing_tables:
        table = dynamodb.create_table(
            TableName='best_books',
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
    dynamodbresourse = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodbresourse.Table('best_books')
    for ind in df.index:
        book = {}
        book['uuid'] = uuid.uuid4().hex
        book['author'] = str(df['author'][ind])
        book['title'] = str(df['title'][ind])
        book['year'] = str(df['year'][ind])
        book['language'] = str(df['language'][ind])
        book['pages'] = str(df['pages'][ind])
        table.put_item(Item=book)
    return 200


def main():
    columns_to_drop = ['country', 'imageLink', 'link']  # TODO: write here which columns you need to drop
    columns_order = ['author', 'title', 'year', 'language', 'pages']  # TODO: write here appropriate order of columns

    object = get_object_from_s3_bucket()
    # object_file = open('100_best_books.json')
    # object = json.load(object_file)

    initial_df = import_data_from_json(object)
    analyzed_df = drop_unnecessary_columns(initial_df, columns_to_drop)
    analyzed_df = reorder_columns_in_data(analyzed_df, columns_order)

    # load language codes from JSON object
    # TODO: write your code here to load language codes to dictionary
    bucket = 'languagebucket'
    key = 'language.json'
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body']

    data_lang = json.loads(content.read())
    lang_dict = data_lang  # this dictionary you will receive as a result of your upload

    analyzed_df = language_transformation(analyzed_df, lang_dict)
    analyzed_df = year_transformation(analyzed_df)

    result = load_data_to_dynamo_db_table(analyzed_df)
    return result


def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
