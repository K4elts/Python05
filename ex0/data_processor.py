from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        super().__init__()
        self._list: list[str] = []
        self._count: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        data = self._list[0]
        counter = self._count
        self._list.pop(0)
        self._count += 1
        return counter, data


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, int | float) and not isinstance(data, bool):
            return True
        if isinstance(data, list) and all(
            isinstance(item, int | float) and not isinstance(item, bool)
                for item in data):
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError
        if isinstance(data, int | float):
            self._list.append(str(data))
        elif isinstance(data, list):
            for item in data:
                self._list.append(str(item))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list) and all(
                isinstance(item, str) for item in data):
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError
        if isinstance(data, str):
            self._list.append(data)
        elif isinstance(data, list):
            for item in data:
                self._list.append(item)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict) and all(
                isinstance(key, str) for key in data.keys()) and all(
                    isinstance(value, str) for value in data.values()):
            return True
        elif isinstance(data, list) and all(
                isinstance(item, dict) and (all(
                    isinstance(key, str) for key in item.keys()) and all(
                        isinstance(value, str) for value in item.values()))
                for item in data):
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError
        if isinstance(data, dict):
            self._list.append(data["log_level"] + ": " + data["log_message"])
        elif isinstance(data, list):
            for item in data:
                self._list.append(
                    item["log_level"] + ": " + item["log_message"])


def test_log_processor() -> None:
    print("Testing LogProcessor...")
    processor_1 = LogProcessor()
    print("Trying to validate input 'Hello':",
          f"{processor_1.validate("Hello")}")
    data_lst = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f"Processing data: {data_lst}")
    processor_1.validate(data_lst)
    processor_1.ingest(data_lst)
    print("Extracting 2 values")
    for _ in range(2):
        rank, value = processor_1.output()
        print(f"Log entry {rank}: {value}")


def test_text_processor() -> None:
    print("Testing TextProcessor...")
    processor_1 = TextProcessor()
    print(f"Trying to validate input '42': {processor_1.validate(42)}")
    data_list = ["Hello", "Nexus", "World"]
    print(f"Processing data: {data_list}")
    processor_1.validate(data_list)
    processor_1.ingest(data_list)
    print("Extracting 1 value...")
    rank, value = processor_1.output()
    print(f"Text value {rank}: {value}")


def test_numeric_processor() -> None:
    print("Testing NumericProcessor...")
    processor_1 = NumericProcessor()
    print(f"Trying to validate input '42': {processor_1.validate(42)}")
    print("Trying to validate input 'Hello':",
          f"{processor_1.validate("Hello")}")
    print("Test invalid ingestion of string 'foo'",
          "without prior validation:")
    try:
        processor_1.ingest("foo")
    except TypeError:
        print("Got exception - Improper numeric data")
    data_list: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_list}")
    processor_1.validate(data_list)
    processor_1.ingest(data_list)
    print("Extracting 3 values...")
    for _ in range(3):
        rank, value = processor_1.output()
        print(f"Numeric Value {rank}: {value}")


def main() -> None:
    print()
    test_numeric_processor()
    print()
    test_text_processor()
    print()
    test_log_processor()


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    main()
