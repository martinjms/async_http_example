from sys import stderr
from aiohttp import client_exceptions, ClientPayloadError, ClientResponseError, ClientSession
from asyncio import Queue, TimeoutError, get_event_loop, gather
from time import time
from async_timeout import timeout

async def write_file(content, filepath, chunk_size):
    with open(filepath, "wb") as fd:
        while True:
            chunk = await content.read(chunk_size)
            if not chunk:
                break
            fd.write(chunk)

async def fetch(session, url, log, filepath, chunk_size = 100000, retry_count=0):
    try:
        async with session.get(url, timeout=50) as response:
            response.raise_for_status()
            log('writing file')
            await write_file(response.content, filepath, chunk_size)
            
               
    except (client_exceptions.ClientConnectorError, ClientPayloadError, TimeoutError) as e:
        if retry_count < 3:
            await fetch(session, url, log, filepath, chunk_size, retry_count + 1)
        else:
            print(str(url) + '  FAILED!', file=stderr)
    except ClientResponseError as e:
        print(str(url) + '  FAILED! - Error code:' + str(e.code), file=stderr)
    except:
        print('Unknown error', file=sys.stderr)

async def reader(filepath):
    print('reader starting...')
    with open(filepath, "r") as f:
        for line in f:
            await queue.put(line.rstrip('\n'))
        for worker in range(1, WORKERS_COUNT):
            await queue.put(None)


async def worker(id, chunk_size):
    def log(st):
        if LOG_ENABLED:
            print('Worker ' + str(id) + ': ' + st)
    log('starting')
    start_time = time()
    url = await queue.get()
    while url:
        filename = str(url).split('/')[-1] + str(id)
        filepath = 'files/' + filename
        log('fetching ' + str(url))
        async with ClientSession() as session:
            content = await fetch(session, url, log, filepath, chunk_size, )
        url = await queue.get()
    log('finished in ' + str(time() - start_time))
       
if __name__ == "__main__":
    tasks = []
    WORKERS_COUNT = 9
    CHUNK_SIZE = 100000
    QUEUE_SIZE = 200
    LOG_ENABLED = True
    queue = Queue(QUEUE_SIZE)
    tasks.extend(map(lambda x: worker(x, CHUNK_SIZE), range(1,WORKERS_COUNT)))
    tasks.append(reader('test_file.txt'))
    loop = get_event_loop()
    loop.run_until_complete(gather(*tasks))
