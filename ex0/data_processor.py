from abc import ABC, abstractmethod
from typing import Any


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

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise IndexError("No data left in processor")
        res = (self._rank, self._data.pop(0))
        self._rank += 1
        return res


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


def numeric_tester() -> None:
    print("Testing Numeric Processor...")

    numeric = NumericProcessor()
    print(" Trying to validate input '42':", numeric.validate(42))
    print(" Trying to validate input 'Hello':", numeric.validate("Hello"))
    print(" Trying to validate input True:", numeric.validate(True))
    print(" Trying to validate input [1, 2, 'hello']:",
          numeric.validate([1, 2, 'hello']))
    print(" Trying to validate input [1, 2, False]:",
          numeric.validate([1, 2, False]))
    print(" Trying to validate input [1, 2, 3.0]:",
          numeric.validate([1, 2, 3.0]))

    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest("foo")
    except TypeError as e:
        print(f" Got exception: {e}")

    print("  Processing data: [1, 2, 3, 4, 5]")
    if numeric.validate([1, 2, 3, 4, 5]):
        numeric.ingest([1, 2, 3, 4, 5])

        print("  Extracting 3 values:")
        for _ in range(3):
            rank, value = numeric.output()
            print(f"  Numeric value {rank}: {value}")
        
        print("   Extract all the rest and 1 more:")
        for _ in range(3):
            try:
                rank, value = numeric.output()
            except IndexError as e:
                print(f"   Error on emtpy processor: {e}")
                return
            print(f"   Numeric value {rank}: {value}")


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    numeric_tester()


if __name__ == "__main__":
    main()
