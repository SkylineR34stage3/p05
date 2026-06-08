from abc import ABC, abstractmethod
from typing import Any, Protocol

_GRN = "\033[92m"  # bright green
_RED = "\033[91m"  # bright red
_YLW = "\033[93m"  # bright yellow
_CYN = "\033[96m"  # bright cyan
_BLD = "\033[1m"   # bold
_RST = "\033[0m"   # reset


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:\n" + ",".join(value for _, value in data))


class JSONExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:\n" +
              "{" +
              ", ".join(f'"item_{rank}": "{value}"' for rank, value in data) +
              "}")


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise IndexError("No data left in processor")
        res = (self._rank, self._data.pop(0))
        self._rank += 1
        return res

    def get_data(self) -> list[str]:
        return self._data

    def get_rank(self) -> int:
        return self._rank


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return all(
                not isinstance(item, bool) and isinstance(item, (int, float))
                for item in data
            )
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._data.append(str(item))
        else:
            self._data.append(str(data))

    def get_name(self) -> str:
        return "Numeric Processor"


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Improper string data")
        if isinstance(data, list):
            for item in data:
                self._data.append(item)
        else:
            self._data.append(data)

    def get_name(self) -> str:
        return "Text Processor"


class LogProcessor(DataProcessor):
    @staticmethod
    def _is_valid_entry(item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        if set(item.keys()) != {"log_level", "log_message"}:
            return False
        return all(isinstance(v, str) for v in item.values())

    def validate(self, data: Any) -> bool:
        if self._is_valid_entry(data):
            return True
        if isinstance(data, list):
            return all(self._is_valid_entry(item) for item in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper dict data")
        items = data if isinstance(data, list) else [data]
        self._data.extend(
            f"{item['log_level']}: {item['log_message']}" for item in items
        )

    def get_name(self) -> str:
        return "Log Processor"


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            accepted = False
            for proc in self._processors:
                if proc.validate(data):
                    proc.ingest(data)
                    accepted = True
                    break
            if not accepted:
                print(f"DataStream error - "
                      f"Can't process element in stream: {data}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            total = proc.get_rank() + len(proc.get_data())
            print(f"{proc.get_name()}: "
                  f"total {total} items processed, "
                  f"remaining {len(proc.get_data())} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            collected: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    collected.append(proc.output())
                except IndexError:
                    break
            plugin.process_output(collected)


def _header(title: str) -> None:
    bar = "═" * 46
    print(f"\n{_BLD}{_CYN}{bar}")
    print(f"  {title}")
    print(f"{bar}{_RST}\n")


def stream_tester() -> None:
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    stream = DataStream()

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message': 'Telnet access!'},
            {'log_level': 'INFO', 'log_message': 'User connected'},
        ],
        42,
        ['Hi', 'five'],
    ]

    batch2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {
                'log_level': 'NOTICE',
                'log_message': 'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]

    print(f"  {_BLD}Initialize Data Stream...\n{_RST}")
    stream.print_processors_stats()

    print(f"\n  {_BLD}Registering Processors:{_RST}")
    stream.register_processor(numeric)
    stream.register_processor(text)
    stream.register_processor(log)

    print(f"\n  {_BLD}Send first batch of data on stream:{_RST} {batch}\n")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print(f"\n  {_BLD}Send 3 processed data "
          f"from each processor to a CSV plugin:{_RST}")
    stream.output_pipeline(3, CSVExport())

    print()
    stream.print_processors_stats()
    print(f"\n  {_BLD}Send another batch of data:{_RST} {batch2}\n")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    print(f"\n  {_BLD}Send 5 processed data "
          f"from each processor to a JSON plugin:{_RST}")
    stream.output_pipeline(5, JSONExport())

    print()
    stream.print_processors_stats()


def main() -> None:
    _header("=== Code Nexus - Data Pipeline ===")
    stream_tester()


if __name__ == "__main__":
    main()
