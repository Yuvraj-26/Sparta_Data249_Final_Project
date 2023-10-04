import boto3
import pandas as pd
import io


def s3_connect(bucket):
    s3_resource = boto3.resource('s3')
    my_bucket = s3_resource.Bucket(bucket)
    return my_bucket


def get_keys_txt(my_bucket, prefix, file_type='txt'):
    keys = []
    for obj in my_bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith(file_type):
            keys.append(obj.key)
    return keys


def read_txt(bucket):
    all_text = []
    for obj in bucket.objects.all():
        if obj.key.endswith('txt'):
            file = obj.get()['Body'].read().decode()
            lines = file.split('\n')
            remove_3 = lines[3:-1]
            all_text.append(remove_3)
    return all_text


def read_csv(bucket, prefix):
    all_text = pd.DataFrame()
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('csv'):
            file = obj.get()['Body'].read().decode()
            df = pd.read_csv(io.StringIO(file))
            all_text = pd.concat([all_text, df])
    return all_text


def write_to_dataframe(file):
    list_person_dict = []
    for item in file:
        for line in item:
            name = line[:line.index(" -  ")].lower().rstrip()
            score_lines = line[line.index(" -  "):].split()
            psychometric_score = score_lines[-3][:score_lines[-3].index('/')]
            presentation_score = score_lines[-1][:score_lines[-1].index('/')]
            person_dict = {'name': name,
                           'psychometric_score': psychometric_score,
                           'presentation_score': presentation_score}
            list_person_dict.append(person_dict)
    df = pd.DataFrame(list_person_dict)
    return df
