"""단일 소유 코어 경계와 폐기한 공유 상태 경로가 다시 생기지 않았는지 검사한다."""
import json,re
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
checks=[]

def require(name,condition,detail):
    checks.append({'name':name,'passed':bool(condition),'detail':detail})
    if not condition:raise AssertionError(name+': '+detail)

rust=list((ROOT/'src').rglob('*.rs'))
code=[]
for path in rust:
    for number,line in enumerate(path.read_text().splitlines(),1):
        stripped=line.lstrip()
        if not stripped.startswith('//'):
            code.append((path.relative_to(ROOT).as_posix(),number,line))
locks=[f'{path}:{line}' for path,line,text in code if re.search(r'\b(?:Mutex|RwLock)\s*<',text)]
require('core_has_no_shared_lock_types',not locks,'공유 Mutex/RwLock 선언 '+str(locks))

retired=['src/domains/cex/engine/mock.rs','src/domains/cex/services/balance_service.rs','src/domains/cex/models/balance.rs','src/shared/database/repositories/cex/balance_repository.rs']
remaining=[path for path in retired if (ROOT/path).exists()]
require('retired_balance_and_mock_paths_are_deleted',not remaining,'남은 폐기 경로 '+str(remaining))

all_source='\n'.join(path.read_text() for path in rust)
references=[name for name in ('MockEngine','BalanceService','UserBalanceRepository') if name in all_source]
require('retired_types_have_no_references',not references,'남은 폐기 타입 '+str(references))

engine=(ROOT/'src/domains/cex/engine/runtime/engine.rs').read_text()
require('core_state_moves_into_one_worker',engine.count('let state = self.state.take()')==1 and engine.count('self.engine_thread = Some(thread::spawn')==1,'상태 이동과 코어 워커 생성은 각각 하나여야 한다')
require('journal_is_a_separate_long_lived_worker',engine.count('self.journal_thread = Some(thread::spawn')==1,'저널 워커는 별도 장수 스레드 하나여야 한다')

output=(ROOT/'src/domains/cex/engine/runtime/output.rs').read_text()
require('output_worker_owns_one_runtime',output.count('Builder::new_current_thread()')==1 and output.count('std::thread::spawn')==1,'출력 워커가 자기 런타임과 장수 스레드를 가져야 한다')

connection=(ROOT/'src/shared/database/connection.rs').read_text()
require('bench_feature_name_matches_cargo','feature = "bench_mode"' in connection and 'feature = "bench"' not in connection,'Cargo의 bench_mode와 조건부 컴파일 이름이 같아야 한다')

result={'checks':checks,'passed':len(checks),'failed':0}
(ROOT.parent/'.local/engine-architecture-audit.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(result,ensure_ascii=False,indent=2))
