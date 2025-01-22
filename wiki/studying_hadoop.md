# 하둡

## 개요

하둡을 공부하면서 찾은 지식, 문제 해결 과정을 기록한다.
누군가에게 보여주기 위한 문서가 아니므로 신속하고 효율적으로 적는 것에 초점을 맞춘다.

또한 잘못된 정보가 있을 수 있음에 유의할 것.

## 하둡에서의 권한 관리, 도커 컨테이너 형태로 실행할 때는 또 어떻게 관리되는지?

### 먼저 HDFS에서의 권한 관리..

> [공식문서](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html)

HDFS는 POSIX의 권한 모델과 상당 부분을 공유한다.

파일
* 파일을 읽으려면 r 권한, 파일에 쓰거나 추가하려면 w 권한 필요
* 파일에 대한 실행 권한은 존재하지 않는다.

디렉터리
* 디렉터리의 항목을 나열하려면 r 권한, 파일이나 디렉터리를 생성하거나 삭제하려면 w 권한, 디렉터리의 하위 항목에 엑세스 하려면 x 권한이 필요하다.

> [HDFS의 권한에 대한 포스트](https://m.blog.naver.com/PostView.naver?isHttpsRedirect=true&blogId=thislover&logNo=220963687570)

HDFS 계층에서는 Linux에서 계정 관리를 하는 것과 같이 생각하면 될 것 같다.

Dockerfile에서 User를 설정해줘야 하나 고민이 들었었는데..
* 일반적으로 hadoop 유저를 생성한 뒤 해당 유저로 하둡을 실행하는 것 같다.
* 물론 docker 컨테이너 안이니까 root여도 사실 host 입장에서는 큰 상관은 없겠지만, 컨테이너 내부에서의 보안을 위해 hadoop 계정을 통해 하둡 데몬을 실행하자.

### 다음으로, HDFS 계층 위에 있는.. YARN에서의 권한 관리

HDFS > YARN > MapReduce

MapReduce는 Hadoop을 최종적으로 사용할 때 수행되는 연산일테니, 결국 Hadoop 클러스터의 컨트롤 센터는 YARN이다.

> 흐름을 생각해보자.

1. 최초에 hadoop을 설치한다.
2. 물리 서버들을 클러스터에 참가시킨다.
    * docker compose를 이용하도록 하고, 각 컨테이너가 어떤 역할을 지녔는지는 xml 파일에 드러난다. (물론 각자의 역할에 맞는 프로세스를 실행해야 함)
3. ssh 인증이 필요하다. 
    * 컨테이너 초기 생성 시 각 컨테이너 간 ssh 키를 생성한 뒤 교환하는 과정이 필요하다. (스크립트로 자동화..)
    * 당연히 아무 서버나 접속하고 그럴 순 없으니까 필요함.
    * Host OS에서도 접속을 해야하니까 (최소한 ResourceManager 컨테이너?) 마찬가지로 키 교환 해준다.
4. 또한 각 노드(도커 환경을 사용한다면 도커 컨테이너, 그렇지 않다면 하나의 OS가 설치된 물리 서버)에 HDFS 유저를 생성해야 한다.
    * Dockerfile을 작성할 때 hadoop 데몬을 실행하는 superuser를 hadoop으로 지정한다.
        * root를 그대로 쓰지 않는 이유는.. hadoop 외의 구성 요소에 대한 권한까지 가지고 있기 때문에 
        * 그리고 다들 hadoop이라는 이름으로 superuser를 생성하는 게 관례인 듯.
    * superuser를 생성했으면, 일반 user도 생성해주자.
        ```
        # 사용자 추가
        useradd -m -s /bin/bash new_user
        usermod -aG hadoop new_user
        # HDFS에 해당 사용자 홈 디렉토리 생성
        hdfs dfs -mkdir /user/new_user
        hdfs dfs -chown new_user:hadoop /user/new_user
        ```
        * Dockerfile에서 일반 유저도 생성하든지, 아니면 노드에 접속 후(혹은 자동화 스크립트) 명령어를 실행해야 하는데
        * 후자의 경우 useradd와 같은 권한이 있어야 하니, root 계정을 사용하고 싶지 않다면 Dockerfile에서 hadoop superuser 생성 시 필요한 권한을 미리 줄 필요가 있다.
            ```
            # hadoop 사용자 생성 및 sudo 권한 부여 (비밀번호 없이)
            RUN useradd -m -s /bin/bash hadoop && \
                echo "hadoop ALL=(ALL) NOPASSWD: /usr/sbin/useradd, /usr/sbin/usermod" >> /etc/sudoers
            ```
5. 이렇게 세팅이 완료되었으면, 어플리케이션 코드를 작성하여 hadoop에게 Map/Reduce 연산 수행을 적절히 요청하면 된다.

-> 근데 다음 날 해보니까 hadoop 유저에 권할을 줘야할 것들이 계속 식별되었다..
* 그냥 sudo 권한을 줘버릴 수도 있겠지만 그러면 root랑 다를 바가 없어짐.
* 그래서 일단 root를 사용하기로 함..


Kerberos 라는 것으로 YARN layer (아마도..?)에서의 계정 관리를 할 수 있다는데 이것도 찾아보자.

## M1 진행할 때 각종 권한 오류, 환경 변수 오류 등 마구 발생함

### 권한 오류..

일단 위에서 적은 정보 대로 하면 얼추 해결이 되지 않을까? (아님 말고)
해보고 안 되면 이어서 적겠다.

### 환경 변수 오류 -> 설정 파일에 대한 이해가 필요

`hadoop-env.sh`에다가 `JAVA_HOME` 환경 변수를 export해줘야 하는 게 이해가 안 되었었다. (왜냐면 Dockerfile에 이미 적었으니까..)

그런데 생각해보니까 hadoop 내부의 프로세스가 shell 프로세스의 환경 변수를 상속받아서 중요한 일에 이용하는 것도 사싦 말이 안 되긴 함

어쨌든 각종 xml, sh 파일들이 어떤 역할을 수행하는지 잘 이해할 필요가 있다.
-> 과제 중에 이와 관련된 것이 있으니까 과제를 하며 학습해보자.

## Map Reduce 어떻게 할 것인지??

하둡 스트리밍 이용 vs Java API(?) 이용

하둡 스트리밍 -> stdin / stdout 이용해서 데이터를 주고 받는다.

### 하둡 스트리밍

일단 mapper랑 reducer를 작성하는데..

input -> (stdin) -> mapper -> (stdout) -> (stdin) -> reducer -> (stdout) -> result

이런 식..