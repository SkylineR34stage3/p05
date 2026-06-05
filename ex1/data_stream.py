from abc import ABC, abstractmethod
from typing import Any

_GRN = "\033[92m"  # bright green
_RED = "\033[91m"  # bright red
_YLW = "\033[93m"  # bright yellow
_CYN = "\033[96m"  # bright cyan
_BLD = "\033[1m"   # bold
_RST = "\033[0m"   # reset


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
                print(f"Can't process element in stream: {data}")

    def print_processors_stats(self) -> None:
        for proc in self._processors:
            total = proc.get_rank() + len(proc.get_data())
            print(f"{proc.get_name()}: "
                  f"total {total} items processed, "
                  f"remaining {len(proc.get_data())} on processor")


def _header(title: str) -> None:
    bar = "═" * 46
    print(f"\n{_BLD}{_CYN}{bar}")
    print(f"  {title}")
    print(f"{bar}{_RST}\n")


def _val(label: str, result: bool, width: int = 18) -> None:
    mark = f"{_GRN}[OK]  True{_RST}" if result else f"{_RED}[KO]  False{_RST}"
    print(f"  validate({_CYN}{label.ljust(width)}{_RST}) -> {mark}")


def numeric_tester() -> None:
    _header("Teting Numeric Processor...")
    numeric = NumericProcessor()

    print(f"  {_BLD}Validation:{_RST}")
    _val("42", numeric.validate(42))
    _val("3.14", numeric.validate(3.14))
    _val("'Hello'", numeric.validate("Hello"))
    _val("True", numeric.validate(True))
    _val("[1, 2, 'hello']", numeric.validate([1, 2, 'hello']))
    _val("[1, 2, False]", numeric.validate([1, 2, False]))
    _val("[1, 2, 3.0]", numeric.validate([1, 2, 3.0]))
    _val("[]", numeric.validate([]))

    print(f"\n  {_BLD}Invalid ingest (no prior validate):{_RST}")
    try:
        numeric.ingest("foo")
    except TypeError as e:
        print(f"  {_YLW}!  ingest('foo') -> {e}{_RST}")

    print(f"\n  {_BLD}Ingest [1, 2, 3, 4, 5]:{_RST}")
    numeric.ingest([1, 2, 3, 4, 5])
    print("  Extracting 3 of 5:")
    for _ in range(3):
        rank, value = numeric.output()
        print(f"    {_CYN}[rank {rank}]{_RST} -> {value}")

    print(f"\n  {_BLD}Extract remaining + 1 extra (empty guard):{_RST}")
    for _ in range(3):
        try:
            rank, value = numeric.output()
            print(f"    {_CYN}[rank {rank}]{_RST} -> {value}")
        except IndexError as e:
            print(f"  {_YLW}!  Empty processor -> {e}{_RST}")
            break


def text_tester() -> None:
    _header("testing Text Processor...")
    text = TextProcessor()

    print(f"  {_BLD}Validation:{_RST}")
    _val("'Hello'", text.validate("Hello"))
    _val("42", text.validate(42))
    _val("True", text.validate(True))
    _val("['Hi', 'five']", text.validate(['Hi', 'five']))
    _val("['hi', 42]", text.validate(['hi', 42]))
    _val("[1, 2, 3]", text.validate([1, 2, 3]))
    _val("[]", text.validate([]))

    print(f"\n  {_BLD}Invalid ingest (no prior validate):{_RST}")
    try:
        text.ingest(42)
    except TypeError as e:
        print(f"  {_YLW}!  ingest(42) -> {e}{_RST}")

    print(f"\n  {_BLD}Ingest ['Hello', 'Nexus', 'World']:{_RST}")
    text.ingest(['Hello', 'Nexus', 'World'])
    print("  Extracting 1 of 3:")
    rank, value = text.output()
    print(f"    {_CYN}[rank {rank}]{_RST} -> {value}")


def log_tester() -> None:
    _header("testing Log Processor...")
    log = LogProcessor()

    width = 110
    print(f"  {_BLD}Validation:{_RST}")
    _val("'Hello'", log.validate("Hello"), width)
    d1 = {'log_level': 'NOTICE'}
    _val(f"{d1}", log.validate(d1), width)
    d2 = {'log_message': 'Something blabla'}
    _val(f"{d2}", log.validate(d2), width)
    d3 = {'log_level': 'NOTICE', 'random_key': 'Something here'}
    _val(f"{d3}", log.validate(d3), width)
    d4 = {'log_level': 'NOTICE', 'log_message': 'Message'}
    _val(f"{d4}", log.validate(d4), width)
    d5 = {'log_level': 'NOTICE', 'log_message': 14}
    _val(f"{d5}", log.validate(d5), width)
    d6 = {'log_level': 'Error', 'log_message': 'Unauthorized!'}
    _val(f"{d6}", log.validate(d6), width)
    l1 = [d4, d5]
    _val(f"{l1}", log.validate(l1), width)
    l2 = [d4, d6]
    _val(f"{l2}", log.validate(l2), width)

    print(f"\n  {_BLD}Invalid ingest (no prior validate):{_RST}")
    try:
        log.ingest(l1)
    except TypeError as e:
        print(f"  {_YLW}!  ingest({l1}) -> {e}{_RST}")

    print(f"\n  {_BLD}Ingest {l2}:{_RST}")
    log.ingest(l2)
    print("  Extracting 2 of 2:")
    for _ in range(2):
        rank, value = log.output()
        print(f"    {_CYN}[rank {rank}]{_RST} -> {value}")

    print("\n  Extracting 1 more (empty guard):")
    try:
        rank, value = log.output()
        print(f"    {_CYN}[rank {rank}]{_RST} -> {value}")
    except IndexError as e:
        print(f"  {_YLW}!  Empty processor -> {e}{_RST}")


def main() -> None:
    print(f"{_BLD}=== Code Nexus - Data Processor ==={_RST}")
    numeric_tester()
    text_tester()
    log_tester()


if __name__ == "__main__":
    main()
