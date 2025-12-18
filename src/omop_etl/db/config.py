from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str | None = None

    def dsn(self) -> str:
        base = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return base + (f"?sslmode={self.sslmode}" if self.sslmode else "")
