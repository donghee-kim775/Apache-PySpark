# Delta Lake와 Pyspark 환경 구축
- 본 환경구축은 Ubuntu 22.04로 실행하였습니다.
1. JAVA 설치
2. 가상환경 만들기
3. pyspark 및 delta lake 설치

### JAVA 설치
```bash
sudo apt update
sudo apt install openjdk-17-jdk
```

### JAVA 환경변수로 등록
```bash
vi .bashrc
```
JAVA_HOME='/usr/lib/jvm/java-1.17.0-openjdk-amd64'
PATH=$PATH:$JAVA_HOME/bin

### 설정되었는지 확인
```bash
source ~/.bashrc
echo $PATH
```

### 가상환경 만들기
```bash
pip install -upgrade pip
sudo apt-get install python3-venv
sudo apt install virtualenv

python3 -m venv deltalakeenv
virtualvenv deltalakeenv --python==python3.8   # 가상환경을 3.8 버전으로 바꿈
echo 'deltalakeenv' >> .gitinore  # git에 가상환경관련 파일들이 업데이트 되지 않도록 함
```

### 가상환경 들어가기
```bash
source deltalakeenv/bin/activate
```

### pyspark와 deltalake 설치하기
```bash
pip3 install pyspark
pip3 install delta-spark==2.2.0
```

---

### Storage Layer
- storage layer - boto3.ipynb : storage layer(AWS S3)로부터 다양한 유형의 데이터를 가져옴

### Processing Layer
- processing layer - converttodelta.ipynb : storage layer로부터 가져온 다양한 유형의 데이터를 deltaTable 형식으로 변환시켜줌
- processing layer - S3toDeltaTable.ipynb : storage layer에 저장된 DeltaTable을 가져와서 timetravel, overwrite 등 여러 기능 사용할 수 있게 해줌
