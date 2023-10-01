# 파이썬 3.9 버전을 사용하는 베이스 이미지를 가져옵니다.
FROM python:3.11-alpine

# /code 디렉터리를 WORKDIR로 설정합니다.
WORKDIR /app

# requirements.txt를 /app 디렉터리에 복사합니다.
COPY ./requirements.txt /app/requirements.txt

# pip를 업그레이드하고, requirements.txt에 명시된 파이썬 패키지를 설치합니다.
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# 모든 파일을 /app 디렉터리에 복사합니다.
COPY ./ /code/

# main.py를 실행합니다.
CMD ["python", "main.py"]
