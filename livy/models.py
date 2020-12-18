import re
from dataclasses import dataclass
from enum import Enum
from functools import total_ordering
from typing import Optional, List


@total_ordering
class Version:
    def __init__(self, version: str) -> None:
        match = re.match(r"(\d+)\.(\d+)\.(\d+)(\S*)$", version)
        if match is None:
            raise ValueError(f"invalid version string {version!r}")
        self.major, self.minor, self.dot, self.extension = match.groups()

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return f"{name}({self.major}.{self.minor}.{self.dot}{self.extension})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Version)
            and self.major == other.major
            and self.minor == other.minor
            and self.dot == other.dot
        )

    def __lt__(self, other: "Version") -> bool:
        if self.major < other.major:
            return True
        elif self.major == other.major:
            if self.minor < other.minor:
                return True
            elif self.minor == other.minor:
                return self.dot < other.dot
            else:
                return False
        else:
            return False


class SparkRuntimeError(Exception):
    def __init__(
        self,
        ename: Optional[str],
        evalue: Optional[str],
        traceback: Optional[List[str]],
    ) -> None:
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback

    def __repr__(self) -> str:
        name = self.__class__.__name__
        components = []
        if self.ename is not None:
            components.append(f"ename={self.ename!r}")
        if self.evalue is not None:
            components.append(f"evalue={self.evalue!r}")
        return f'{name}({", ".join(components)})'


class OutputStatus(Enum):
    OK = "ok"
    ERROR = "error"


@dataclass
class Output:
    status: OutputStatus
    text: Optional[str]
    json: Optional[dict]
    ename: Optional[str]
    evalue: Optional[str]
    traceback: Optional[List[str]]

    @classmethod
    def from_json(cls, data: dict) -> "Output":
        return cls(
            OutputStatus(data["status"]),
            data.get("data", {}).get("text/plain"),
            data.get("data", {}).get("application/json"),
            data.get("ename"),
            data.get("evalue"),
            data.get("traceback"),
        )

    def raise_for_status(self) -> None:
        if self.status == OutputStatus.ERROR:
            raise SparkRuntimeError(self.ename, self.evalue, self.traceback)


class StatementKind(Enum):
    SPARK = "spark"
    PYSPARK = "pyspark"
    SPARKR = "sparkr"
    SQL = "sql"


class StatementState(Enum):
    WAITING = "waiting"
    RUNNING = "running"
    AVAILABLE = "available"
    ERROR = "error"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


@dataclass
class Statement:
    session_id: int
    statement_id: int
    state: StatementState
    code: str
    output: Optional[Output]
    progress: Optional[float]

    @classmethod
    def from_json(cls, session_id: int, data: dict) -> "Statement":
        if data["output"] is None:
            output = None
        else:
            output = Output.from_json(data["output"])

        return cls(
            session_id,
            data["id"],
            StatementState(data["state"]),
            data["code"],
            output,
            data.get("progress"),
        )


class SessionKind(Enum):
    SPARK = "spark"
    PYSPARK = "pyspark"
    PYSPARK3 = "pyspark3"
    SPARKR = "sparkr"
    SQL = "sql"
    SHARED = "shared"


# Possible session states are defined here:
# https://github.com/apache/incubator-livy/blob/master/core/src/main/scala/
# org/apache/livy/sessions/SessionState.scala
class SessionState(Enum):
    NOT_STARTED = "not_started"
    STARTING = "starting"
    RECOVERING = "recovering"
    IDLE = "idle"
    RUNNING = "running"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"


SESSION_STATE_NOT_READY = {SessionState.NOT_STARTED, SessionState.STARTING}
SESSION_STATE_FINISHED = {
    SessionState.ERROR,
    SessionState.DEAD,
    SessionState.KILLED,
    SessionState.SUCCESS,
}


@dataclass
class Session:
    session_id: int
    proxy_user: str
    kind: SessionKind
    state: SessionState

    @classmethod
    def from_json(cls, data: dict) -> "Session":
        return cls(
            data["id"],
            data["proxyUser"],
            SessionKind(data["kind"]),
            SessionState(data["state"]),
        )


@dataclass
class Batch:
    batch_id: int
    app_id: Optional[str]
    app_info: Optional[dict]
    log: List[str]
    state: SessionState

    @classmethod
    def from_json(cls, data: dict) -> "Batch":
        return cls(
            data["id"],
            data.get("appId"),
            data.get("appInfo"),
            data.get("log", []),
            SessionState(data["state"]),
        )


@dataclass
class BatchLog:
    batch_id: int
    from_: int
    total: int
    lines: List[str]

    @classmethod
    def from_json(cls, data: dict) -> "BatchLog":
        return cls(
            data["id"], data["from"], data["total"], data.get("log", [])
        )
