"""실제 기록에서 발견한 이름/외부 로그/성공 표기 분리와 누락 집계 회귀를 검증한다."""
import unittest
from rust_test_results import parse_log


class RustTestResultsTest(unittest.TestCase):
    def test_plain_success(self):
        result = parse_log("test module::case ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
        self.assertEqual(result["cases"], ["module::case"])
        self.assertEqual(result["tests"], 1)

    def test_interleaved_broker_failure_log_does_not_hide_passing_case(self):
        result = parse_log("test module::startup ... %3|FAIL|rdkafka: 격리 주소 연결 거절\nok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
        self.assertEqual(result["cases"], ["module::startup"])
        self.assertEqual(result["failed"], 0)

    def test_missing_or_duplicate_names_cannot_match_summary(self):
        for names in ["test one ... ok\n", "test one ... ok\ntest one ... ok\n"]:
            with self.assertRaises(ValueError):
                parse_log(names + "test result: ok. 2 passed; 0 failed; 0 ignored;\n")

    def test_failed_or_ignored_summary_is_rejected(self):
        for summary in ["FAILED. 1 passed; 1 failed; 0 ignored;", "ok. 1 passed; 0 failed; 1 ignored;"]:
            with self.assertRaises(ValueError):
                parse_log("test one ... ok\ntest result: " + summary + "\n")

    def test_zero_selection_and_missing_summary_are_rejected(self):
        for text in ["test result: ok. 0 passed; 0 failed; 0 ignored;\n", "test one ... ok\n"]:
            with self.assertRaises(ValueError):
                parse_log(text)


if __name__ == "__main__":
    unittest.main()
