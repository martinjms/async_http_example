import asyncio
from aiohttp import web

@asyncio.coroutine
def random_file(request):
    return web.Response(body=b"Hello, world")

app = web.Application()
app.router.add_route('GET', '/', random_file)
app.router.add_static('/static', './public')


loop = asyncio.get_event_loop()
f = loop.create_server(app.make_handler(), '0.0.0.0', 8080)
srv = loop.run_until_complete(f)
print('serving on', srv.sockets[0].getsockname())
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass
