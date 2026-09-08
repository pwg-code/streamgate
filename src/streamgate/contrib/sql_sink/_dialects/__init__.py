"""方言层（内部）：sqlite（dev）/ mssql（生产）幂等 upsert 语句构造。

非公共 API：方言选择由 DbConfig.connection_string 驱动，
面向用户的入口为 streamgate.contrib.sqlite_sink 与 streamgate.contrib.mssql_sink。
"""
