import os
import sys

sys.path.append('/app')


c = get_config()

# 모든 IP에서 접속 허용
c.ServerApp.ip = '0.0.0.0'

# 포트 설정
c.ServerApp.port = 8888

# 작업 디렉토리 설정
c.ServerApp.root_dir = '/app'
c.ServerApp.notebook_dir = '/app'