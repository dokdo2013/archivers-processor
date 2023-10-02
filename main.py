import os
import uuid
from datetime import datetime
import requests
import boto3
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from model import YudarlinnSegment
import dotenv

dotenv.load_dotenv()

# 1. 환경변수 검증
REQUIRED_ENV_VARS = ['STREAM_ID', 'DATE', 'SEGMENT_URI', 'SEGMENT_DURATION',
                     'S3_URL', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY',
                     'S3_BUCKET_NAME', 'DB_DIALECT', 'DB_HOST', 'DB_PORT',
                     'DB_USERNAME', 'DB_PASSWORD', 'DB_DATABASE', 'CDN_BASE_URL']

for var in REQUIRED_ENV_VARS:
    if var not in os.environ:
        raise ValueError(f"환경변수 {var}이/가 설정되지 않았습니다.")

print("[1] 환경변수 검증 완료", os.environ['STREAM_ID'])

# 2. SEGMENT_URI로 ts 파일 다운로드 후 S3 업로드
ts_file_name = os.path.basename(os.environ['SEGMENT_URI'])
uuid_file_name = uuid.uuid4()
upload_file_name = f"{uuid_file_name}.ts"

response = requests.get(os.environ['SEGMENT_URI'], stream=True)
with open(upload_file_name, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

print("[2] ts 파일 다운로드 완료", upload_file_name)

# S3 업로드
s3 = boto3.client('s3',
                  region_name="auto",
                  endpoint_url=os.environ['S3_URL'],
                  aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
                  aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'])
with open(upload_file_name, 'rb') as f:
    s3.upload_fileobj(f, os.environ['S3_BUCKET_NAME'], f"{os.environ['STREAM_ID']}/{upload_file_name}")

print("[3] S3 업로드 완료")

# 3. DB 연결 및 데이터 추가
dialect = 'postgresql' if os.environ['DB_DIALECT'] == 'postgres' else os.environ['DB_DIALECT']
engine = create_engine(f"{dialect}://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_DATABASE']}")

print("[4] DB 연결 완료")

# 데이터 추가
s3_link = f"{os.environ['S3_URL']}/{os.environ['S3_BUCKET_NAME']}/{upload_file_name}"
converted_date = datetime.strptime(os.environ['DATE'], '%Y-%m-%dT%H:%M:%S+00:00')
current_date = datetime.utcnow()
new_segment = YudarlinnSegment(
    stream_id=os.environ['STREAM_ID'],
    segment_id=str(uuid_file_name),
    segment_length=float(os.environ['SEGMENT_DURATION']),
    link=f"{os.environ['CDN_BASE_URL']}/{os.environ['STREAM_ID']}/{upload_file_name}",
    created_at=converted_date,
    updated_at=current_date
)

session = Session(engine)
session.add(new_segment)
session.commit()
session.close()

print("[5] DB 데이터 추가 완료")
