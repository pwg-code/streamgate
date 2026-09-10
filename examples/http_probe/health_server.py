"""零依赖健康端点演示（stdlib asyncio TCP 服务，demo 品质）。

生产请换你自己的 Web 框架路由（streamgate 核心不含任何 HTTP 代码——
健康数据归框架的 health_snapshot()，暴露方式归使用方）。
"""

import asyncio

from streamgate import Consumer, logger


async def serve_consumer_health(consumer: Consumer, port: int) -> None:
    """后台托管 GET /health：返回 consumer.health_snapshot() 的 JSON。"""

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            path = request_line.decode("latin-1").split(" ")[1] if request_line else ""
            # 消费请求头（demo 不读内容，仅排空至空行）
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
            if path != "/health":
                body = b'{"error": "not found"}'
                status = "404 Not Found"
            else:
                snapshot = await consumer.health_snapshot()
                body = snapshot.model_dump_json().encode("utf-8")
                status = "200 OK"
            writer.write(
                b"HTTP/1.1 " + status.encode() + b"\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body
            )
            await writer.drain()
        except Exception as e:
            logger.debug("health_endpoint_error", error=str(e))
        finally:
            writer.close()

    server = await asyncio.start_server(_handle, "0.0.0.0", port)
    logger.info("health_endpoint_listening", port=port)
    async with server:
        await server.serve_forever()
