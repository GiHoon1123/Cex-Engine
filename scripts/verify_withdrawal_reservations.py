"""소유 PostgreSQL/Kafka/실제 Rust에서 출금 예약·완료·해제와 강제 종료 뒤 자체 복구를 검증한다.

플랫폼 원장/승인이나 BTC 서명·전송을 대신하는 시험이 아니다. DB는 최초 검증 잔액만 제공한다.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request,urlopen
from uuid import uuid4
from isolated_kafka import IsolatedKafka
ROOT=Path(__file__).resolve().parent.parent
ARTIFACTS=ROOT.parent/'.local';run=uuid4().hex[:12];logs=ARTIFACTS/'withdrawal-reservations'/run;logs.mkdir(parents=True)
DOCKER=shutil.which('docker') or '/usr/local/bin/docker';CARGO=shutil.which('cargo') or str(Path.home()/'.cargo/bin/cargo')
pg='cex-withdrawal-'+run;broker=IsolatedKafka('cex-withdrawal');processes=[];token=uuid4().hex+uuid4().hex;audit=uuid4().hex+uuid4().hex
result={'run_id':run,'database_container':pg,'broker_container':broker.name,'checks':[]}
with socket.socket() as reserved:reserved.bind(('127.0.0.1',0));port=reserved.getsockname()[1]
engine=None

def sql(statement):return subprocess.check_output([DOCKER,'exec',pg,'psql','-U','root','-d','cex','-v','ON_ERROR_STOP=1','-Atc',statement],text=True).strip()
def http(path,body=None,key=None):
 request=Request('http://127.0.0.1:'+str(port)+path,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json','x-engine-service-token':token if key is None else key,'x-engine-audit-token':audit})
 try:response=urlopen(request,timeout=7)
 except HTTPError as error:response=error
 with response:
  raw=response.read()
  if response.status==401:
   assert raw==b'', '서비스 인증 거절은 기존 빈 응답 계약이다'
   return response.status,None
  return response.status,json.loads(raw)
def ready():
 deadline=time.monotonic()+40
 while time.monotonic()<deadline:
  if engine.poll() is not None:raise RuntimeError('소유 엔진 기동 실패: '+str(logs))
  try:
   if http('/api/health')[0]==200:return
  except (URLError,OSError):pass
  time.sleep(.1)
 raise RuntimeError('소유 엔진 준비 시간 초과')
def start(env):
 global engine
 with (logs/('engine-'+str(len(processes))+'.log')).open('w') as log:engine=subprocess.Popen([str(ROOT/'target/debug/cex-engine')],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
 processes.append(engine);ready()
def change(identity,action,amount='0.4',status=200):
 response,body=http('/api/cex/withdrawals/reservations',{'withdrawal_id':identity,'user_id':1,'asset':'BTC','amount':amount,'action':action})
 assert response==status,(response,body)
 if status==200:assert body=={'withdrawal_id':identity,'action':action,'code':{'reserve':'DURABLY_RESERVED','release':'DURABLY_RELEASED','complete':'DURABLY_COMPLETED'}[action]}
 else:assert body['code']=='DURABLY_REJECTED'
def journal():
 status,page=http('/internal/audit/results?after=0&limit=100');assert status==200;return page
try:
 with (logs/'build.log').open('w') as log:subprocess.run([CARGO,'build','--locked','--offline','--bin','cex-engine'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
 subprocess.run([DOCKER,'run','--detach','--rm','--name',pg,'--publish','127.0.0.1::5432','--tmpfs','/var/lib/postgresql/data','-e','POSTGRES_USER=root','-e','POSTGRES_PASSWORD=1234','-e','POSTGRES_DB=cex','postgres:16'],check=True,stdout=subprocess.DEVNULL)
 deadline=time.monotonic()+45
 while subprocess.run([DOCKER,'exec',pg,'pg_isready','-h','127.0.0.1','-U','root','-d','cex'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
  if time.monotonic()>deadline:raise RuntimeError('소유 DB 준비 실패')
  time.sleep(.2)
 address=subprocess.check_output([DOCKER,'port',pg,'5432/tcp'],text=True).strip()
 # 실제 최초 기동이 읽는 최소 표다. 플랫폼의 전체 업무 스키마를 검증했다고 주장하지 않는다.
 sql("CREATE TABLE user_balances(user_id bigint,mint_address text,available numeric(30,9),locked numeric(30,9));INSERT INTO user_balances VALUES(1,'BTC',1,0);CREATE TABLE orders(id bigint,status text);CREATE TABLE engine_order_receipts(order_id bigint);CREATE TABLE engine_event_receipts(event_id text);CREATE TABLE bitcoin_credit_dispatch_attempts(id bigint);")
 broker.start()
 with tempfile.TemporaryDirectory(prefix='cex-withdrawal-journal-') as directory:
  env={k:v for k,v in os.environ.items() if not k.startswith(('ENGINE_','DATABASE_','KAFKA_'))};env.update(ENGINE_SERVICE_TOKEN=token,ENGINE_AUDIT_TOKEN=audit,DATABASE_HOST=address,ENGINE_HTTP_ADDRESS='127.0.0.1:'+str(port),KAFKA_BOOTSTRAP_SERVERS=broker.bootstrap,ENGINE_DATA_DIR=directory)
  start(env);assert journal()['through']==0
  status,_=http('/api/cex/withdrawals/reservations',{'withdrawal_id':str(uuid4()),'user_id':1,'asset':'BTC','amount':'0.4','action':'reserve'},key='wrong-identity');assert status==401;assert journal()['through']==0
  result['checks'].append({'case':'unauthorized_command_has_no_journal_record','status':401})
  released,completed,rejected=map(str,[uuid4(),uuid4(),uuid4()]);change(released,'reserve');change(released,'reserve');before=journal();assert len(before['events'])==1
  engine.kill();engine.wait(timeout=10);subprocess.run([DOCKER,'pause',pg],check=True,stdout=subprocess.DEVNULL);start(env)
  assert journal()['events']==before['events'];change(released,'reserve');change(released,'reserve','0.5',409);change(released,'release');change(released,'release');change(released,'complete',status=409)
  change(completed,'reserve','0.3');change(completed,'complete','0.3');change(completed,'complete','0.3');change(completed,'release','0.3',409);change(completed,'reserve','0.3',409)
  change(rejected,'reserve','2',409);status,_=http('/api/cex/deposits/credits',{'deposit_id':str(uuid4()),'user_id':1,'asset':'BTC','amount':'3'});assert status==200;change(rejected,'reserve','2',409)
  after=journal();assert len(after['events'])==6
  result['checks'].append({'case':'own_wal_recovers_while_initial_database_is_paused','events':6,'original_event_unchanged':True})
  engine.kill();engine.wait(timeout=10);start(env);assert journal()['events']==after['events'];change(released,'release');change(completed,'complete','0.3');change(rejected,'reserve','2',409);assert journal()['events']==after['events']
  # 브로커 전달 커서는 정상 출력 워커가 저장한다. 강제 종료 중 중복은 허용하되 동일 ID/내용만 가능하다.
  with (logs/'kafka-events.log').open('w') as out,(logs/'kafka-consumer.log').open('w') as err:
   consumed=subprocess.run([DOCKER,'exec',broker.name,'/opt/kafka/bin/kafka-console-consumer.sh','--bootstrap-server','localhost:29092','--topic','order-events','--from-beginning','--timeout-ms','5000','--max-messages','30'],stdout=out,stderr=err,timeout=25)
  assert consumed.returncode in (0,1),consumed.returncode
  if consumed.returncode==1:assert 'TimeoutException' in (logs/'kafka-consumer.log').read_text()
  records=[json.loads(line) for line in (logs/'kafka-events.log').read_text().splitlines() if line.startswith('{')];unique={}
  for record in records:
   identity=record['event_id'];assert identity not in unique or unique[identity]==record;unique[identity]=record
  actual=[json.loads(value) for value in after['events']];assert len(unique)==6;assert set(unique)=={value['event_id'] for value in actual};assert all(unique[value['event_id']]==value for value in actual)
  assert [value['event_type'] for value in actual]==['withdrawal_reserved','withdrawal_released','withdrawal_reserved','withdrawal_completed','withdrawal_rejected','deposit_credited']
  result['checks'].append({'case':'actual_kafka_equals_durable_audit_events','raw_events':len(records),'unique_events':6,'consumer_exit_after_quiet_timeout':consumed.returncode})
  engine.kill();engine.wait(timeout=10);subprocess.run([DOCKER,'unpause',pg],check=True,stdout=subprocess.DEVNULL);assert sql("SELECT available::text||':'||locked::text FROM user_balances WHERE user_id=1")=='1.000000000:0.000000000'
  result['checks'].append({'case':'engine_does_not_write_platform_projection','initial_balance_unchanged':True})
  for file in Path(directory).iterdir():
   if file.is_file():shutil.copy2(file,logs/file.name)
 result['exit']=0
finally:
 for process in reversed(processes):
  if process.poll() is None:process.kill();process.wait(timeout=10)
 result['broker_removed']=broker.stop();subprocess.run([DOCKER,'rm','--force',pg],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);result['database_removed']=subprocess.run([DOCKER,'inspect',pg],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode!=0
 (ARTIFACTS/'withdrawal-reservations.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps(result,ensure_ascii=False,indent=2),flush=True)
