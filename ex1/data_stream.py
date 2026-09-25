from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        super().__init__()
        self._list: list[str] = []
        self._count = 0
        self._processed = 0

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

    def get_total(self) -> int:
        return self._processed

    def get_list_len(self) -> int:
        return len(self._list)


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
            self._processed += 1
            self._list.append(str(data))
        elif isinstance(data, list):
            for item in data:
                self._processed += 1
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
            self._processed += 1
            self._list.append(data)
        elif isinstance(data, list):
            for item in data:
                self._processed += 1
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
            self._processed += 1
            self._list.append(data["log_level"] + ": " + data["log_message"])
        elif isinstance(data, list):
            for item in data:
                self._processed += 1
                self._list.append(
                    item["log_level"] + ": " + item["log_message"])


class DataStream:
    def __init__(self) -> None:
        self._processor: list[DataProcessor] = []

    def register_processors(self, proc: DataProcessor) -> None:
        self._processor.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            for proc in self._processor:
                if proc.validate(item):
                    proc.ingest(item)
                    break
            else:
                print("DataStream error - Can't procces element in stream:",
                      f"{item}")

    def print_processor_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processor:
            print("No processor found, no data")
            return
        for proc in self._processor:
            name = type(proc).__name__.replace("Processor", " Processor")
            print(f"{name}: total {proc.get_total()} items processed",
                  f"remaining {proc.get_list_len()} on processor")


def main() -> None:
    print()
    print("Initialize Data Stream...")
    data_list = ['Hello world', [3.14, -1, 2.71],
                 [{'log_level': 'WARNING',
                   'log_message': 'Telnet access! Use ssh instead'},
                  {'log_level': 'INFO',
                  'log_message': 'User wil isconnected'}],
                 42,
                 ['Hi', 'five']]
    data_stream = DataStream()
    num_processor = NumericProcessor()
    data_stream.register_processors(num_processor)
    data_stream.process_stream(data_list)
    data_stream.print_processor_stats()
    print()
    print("Registering other data processors")
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processors(text_processor)
    data_stream.register_processors(log_processor)
    print("Sending the same data again")
    data_stream.process_stream(data_list)
    data_stream.print_processor_stats()
    print()
    print("Consume some elements from the data processor:"
          "Numeric 3 Text 2 Log 1")
    for _ in range(3):
        num_processor.output()
    for _ in range(2):
        text_processor.output()
    log_processor.output()
    data_stream.print_processor_stats()


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    main()
