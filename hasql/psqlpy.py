from typing import Sequence

from psqlpy import ConnectionPool, Connection, QueryResult

from hasql.base import BasePoolManager
from hasql.metrics import DriverMetrics
from hasql.utils import Dsn


class PoolManager(BasePoolManager):
    pools: Sequence[ConnectionPool]

    def get_pool_freesize(self, pool: ConnectionPool):
        return pool.status().available

    async def acquire_from_pool(self, pool: ConnectionPool, **kwargs):
        return pool.acquire(**kwargs)

    async def release_to_pool(self, connection: Connection, pool: ConnectionPool, **kwargs):
        connection.back_to_pool(**kwargs)

    async def _is_master(self, connection: Connection) -> bool:
        query_result: QueryResult = await connection.execute(
            'SHOW transaction_read_only',[],
        )
        return query_result.result()[0] == "off"

    async def _pool_factory(self, dsn: Dsn):
        return ConnectionPool(dsn=str(dsn), **self.pool_factory_kwargs)

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["max_db_pool_size"] = kwargs.pop("max_db_pool_size", 10) + 1
        return kwargs

    async def _close(self, pool: ConnectionPool):
        pool.close()

    async def _terminate(self, pool: ConnectionPool):
        pass

    def is_connection_closed(self, connection):
        return connection.is_closed()

    def host(self, pool: ConnectionPool):
        return ''  # not

    def _driver_metrics(self) -> Sequence[DriverMetrics]:
        driver_metrics = []
        for pool in self.pools:
            pool_status = pool.status()
            driver_metrics.append(
                DriverMetrics(
                    max=pool_status.max_size,
                    min=1,  # not supported by psqlpy
                    idle=pool_status.available,
                    used=pool_status.max_size - pool_status.available,
                    host=self.host(pool),
                )
            )
        return driver_metrics


__all__ = ("PoolManager",)
