import json


import pandas as pd
import boto3

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
        book = [str(record['author']), str(record['country']), str(record['imageLink']), str(record['link']),
                int(record['pages']), str(record['title']), str(record['year'])]
        book_list.append(book)

    df = pd.DataFrame(book_list, columns=['author', 'country', 'imageLink', 'link', 'pages', 'title', 'year'])
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
            new_lang.append(lang_dict[lang])
        fin_str = ""
        for lang in new_lang:
            fin_str += lang
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
            'AttributeName': 'date',
            'KeyType': 'RANGE'
        }
    ]
    attribute_definitions = [
        {
            'AttributeName': 'phrase',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'uuid',
            'AttributeType': 'S'
        }
    ]
    table = dynamodb.create_table(
        TableName='best_books',
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={
            'ReadCapacityUnits': 110,
            'WriteCapacityUnits': 110
        }
    )
    for index, row in df.iterrows():
        phraseType = ''
        if row[0].count(' ') > 0:
            phraseType = 'Phrase'
        else:
            phraseType = 'Word'
        chunk = {"phrase": row[0], "type": phraseType, 'uuid': str()}
        print(chunk)
        table.put_item(Item=chunk)


def main():
    columns_to_drop = []  # TODO: write here which columns you need to drop
    columns_order = []  # TODO: write here appropriate order of columns

    object = get_object_from_s3_bucket()

    initial_df = import_data_from_json(object)
    analyzed_df = drop_unnecessary_columns(initial_df, columns_to_drop)
    analyzed_df = reorder_columns_in_data(analyzed_df, columns_order)

    # load language codes from JSON object
    # TODO: write your code here to load language codes to dictionary
    lang_dict = {}  # this dictionary you will receive as a result of your upload

    analyzed_df = language_transformation(analyzed_df, lang_dict)
    analyzed_df = year_transformation(analyzed_df)

    result = load_data_to_dynamo_db_table(analyzed_df)
    return result


if __name__ == '__main__':
    main()
