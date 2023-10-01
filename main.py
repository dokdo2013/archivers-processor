import os
import random
from datetime import datetime
import requests
import boto3
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from model import YudarlinnSegment

# 1. 환경변수 검증
REQUIRED_ENV_VARS = ['STREAM_ID', 'DATE', 'SEGMENT_URI', 'SEGMENT_DURATION',
                     'S3_URL', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY',
                     'S3_BUCKET_NAME', 'DB_DIALECT', 'DB_HOST', 'DB_PORT',
                     'DB_USERNAME', 'DB_PASSWORD', 'DB_DATABASE']

for var in REQUIRED_ENV_VARS:
    if var not in os.environ:
        raise ValueError(f"환경변수 {var}이/가 설정되지 않았습니다.")

# 2. SEGMENT_URI로 ts 파일 다운로드 후 S3 업로드
ts_file_name = os.path.basename(os.environ['SEGMENT_URI'])
response = requests.get(os.environ['SEGMENT_URI'], stream=True)
with open(ts_file_name, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

# S3 업로드
s3 = boto3.client('s3',
                  endpoint_url=os.environ['S3_URL'],
                  aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
                  aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'])
random_string = hex(int(datetime.utcnow().strftime('%Y%m%d%H%M%S%f').zfill(20)) + random.randint(0, 9999))[2:]
shuffled_random_string = ''.join(random.sample(random_string, len(random_string)))
upload_file_name = f"{shuffled_random_string}.ts"
with open(ts_file_name, 'rb') as f:
    s3.upload_fileobj(f, os.environ['S3_BUCKET_NAME'], upload_file_name)


# 3. DB 연결 및 데이터 추가
engine = create_engine(f"{os.environ['DB_DIALECT']}://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_DATABASE']}")

# 데이터 추가
s3_link = f"{os.environ['S3_URL']}/{os.environ['S3_BUCKET_NAME']}/{upload_file_name}"
converted_date = datetime.strptime(os.environ['DATE'], '%Y%m%dT%H:%M:%SZ')
current_date = datetime.utcnow()
new_segment = YudarlinnSegment(
    stream_id=os.environ['STREAM_ID'],
    segment_id=shuffled_random_string,
    segment_length=float(os.environ['SEGMENT_DURATION']),
    link=s3_link,
    created_at=converted_date,
    updated_at=current_date
)

session = Session(engine)
session.add(new_segment)
session.commit()
session.close()
