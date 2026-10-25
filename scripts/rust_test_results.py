"""표준 시험 요약과 이름을 함께 대조한다. 외부 로그가 성공 표기를 끊어도 시험을 누락하지 않는다."""
import re


def parse_log(text):
    # 카프카의 표준 오류가 이름과 ok 사이에 들어올 수 있어 한 줄 끝의 ok만 찾지 않는다.
    names = re.findall(r"^test (\S+) \.\.\.", text, re.MULTILINE)
    summaries = re.findall(
        r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", text, re.MULTILINE
    )
    if not summaries:
        raise ValueError("시험 요약이 없습니다")
    passed = sum(int(row[1]) for row in summaries)
    if passed == 0 or any(row[0] != "ok" or int(row[2]) or int(row[3]) for row in summaries):
        raise ValueError("실패·제외·빈 선택은 완료로 집계할 수 없습니다")
    if len(names) != passed or len(set(names)) != len(names):
        raise ValueError("시험 이름과 통과 개수가 일치하지 않습니다")
    return {"tests": passed, "failed": 0, "ignored": 0, "cases": names}
