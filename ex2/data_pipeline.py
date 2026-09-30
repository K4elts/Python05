from abc import ABC, abstractmethod
from typing import Any, Protocol


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class DataStream:
    def __init__(self) -> None:
        self._processor: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processor.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            for proc in self._processor:
                if proc.validate(item):
                    proc.ingest(item)
                    break
            else:
                print("DataStream error - Can't proccess element in stream:",
                      f"{item}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processor:
            print("No processor found, no data")
            return
        for proc in self._processor:
            name = type(proc).__name__.replace("Processor", " Processor")
            print(f"{name}: total {proc.get_total()} items processed",
                  f"remaining {proc.get_list_len()} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processor:
            new_list = []
            for _ in range(nb):
                if proc.get_list_len() > 0:
                    key, value = proc.output()
                    new_list.append((key, value))
            plugin.process_output(new_list)


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = []
        for item in data:
            values.append(item[1])
        result = ",".join(values)
        print("CSV Output:")
        print(result)


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = []
        for key, value in data:
            values.append(f'"item_{key}": "{value}"')
        result = ", ".join(values)
        result = "{" + result + "}"
        print("JSON Output:")
        print(result)


def main() -> None:
    print()
    print("Initialize Data Stream...")
    data_list = ['Hello world', [3.14, -1, 2.71],
                 [{'log_level': 'WARNING',
                   'log_message': 'Telnet access! Use ssh instead'},
                  {'log_level': 'INFO',
                  'log_message': 'User wil is connected'}],
                 42,
                 ['Hi', 'five']]
    data_stream = DataStream()
    print()
    data_stream.print_processors_stats()
    print()
    print("Registering processors\n")
    num_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processor(num_processor)
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)
    print(f"Sending first batch of stream of data: {data_list}")
    data_stream.process_stream(data_list)
    print()
    data_stream.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to a CSV Plugin")
    csv_plugin = CSVPlugin()
    json_plugin = JSONPlugin()
    data_stream.output_pipeline(3, csv_plugin)
    print()
    data_stream.print_processors_stats()
    data_list_2 = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
                   [{'log_level': 'ERROR', 'log_message': '500 server crash'},
                    {'log_level': 'NOTICE',
                     'log_message': 'Certificate expires in 10 days'}],
                   [32, 42, 64, 84, 128, 168], 'World hello']
    print()
    print(f"Send another batch of data: {data_list_2}")
    data_stream.process_stream(data_list_2)
    print()
    data_stream.print_processors_stats()
    print()
    print("Send 5 processed data from each processor to a JSON plugin")
    data_stream.output_pipeline(5, json_plugin)
    print()
    data_stream.print_processors_stats()


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    main()
