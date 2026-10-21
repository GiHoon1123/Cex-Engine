"""현재 검증이 소유한 브로커만 만들고 제거한다. 기존 로컬 브로커는 조회하지 않는다."""
import shutil
import socket
import subprocess
import time
from uuid import uuid4

class IsolatedKafka:
    def __init__(self, prefix):
        self.docker=shutil.which("docker") or "/usr/local/bin/docker"
        self.name=prefix+"-"+uuid4().hex[:10]
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1",0));self.port=reserve.getsockname()[1]
        self.bootstrap=f"127.0.0.1:{self.port}"
    def command(self,*args,**kwargs):
        return subprocess.run([self.docker,*args],check=True,**kwargs)
    def start(self):
        env={"KAFKA_NODE_ID":"1","KAFKA_PROCESS_ROLES":"broker,controller",
            "KAFKA_LISTENERS":"EXTERNAL://:9092,INTERNAL://:29092,CONTROLLER://:9093",
            "KAFKA_ADVERTISED_LISTENERS":f"EXTERNAL://{self.bootstrap},INTERNAL://localhost:29092",
            "KAFKA_CONTROLLER_LISTENER_NAMES":"CONTROLLER","KAFKA_INTER_BROKER_LISTENER_NAME":"INTERNAL",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":"EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS":"1@localhost:9093","KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":"1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":"1","KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":"1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":"0","KAFKA_NUM_PARTITIONS":"1"}
        args=["run","--detach","--rm","--name",self.name,"--publish",f"127.0.0.1:{self.port}:9092"]
        for key,value in env.items():args.extend(["--env",f"{key}={value}"])
        self.command(*args,"apache/kafka:3.9.1",stdout=subprocess.DEVNULL)
        try:
            deadline=time.monotonic()+60
            while True:
                result=subprocess.run([self.docker,"exec",self.name,"/opt/kafka/bin/kafka-topics.sh","--bootstrap-server","localhost:29092","--list"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10)
                if result.returncode==0:break
                if time.monotonic()>deadline:raise RuntimeError("격리 브로커 시작 시간 초과")
                time.sleep(0.5)
            for topic in ["order-events","market-events"]:
                self.command("exec",self.name,"/opt/kafka/bin/kafka-topics.sh","--bootstrap-server","localhost:29092","--create","--topic",topic,"--partitions","1","--replication-factor","1",stdout=subprocess.DEVNULL)
            return self
        except BaseException:
            self.stop();raise
    def stop(self):
        subprocess.run([self.docker,"rm","--force",self.name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        return subprocess.run([self.docker,"inspect",self.name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode!=0
