import asyncio
from proxybroker import Broker

proxy_list = []
with open("proxies.txt", "w") as file:
    async def show(proxies):
        while True:
            proxy = await proxies.get()
            if proxy is None: break
            proxy_list.append(proxy)
            file.write(proxy.host + ":" + str(proxy.port) +"\n")
            print('Found proxy: %s' % proxy)


    proxies = asyncio.Queue()
    broker = Broker(proxies)
    tasks = asyncio.gather(
        broker.find(types=['HTTP', 'HTTPS'], limit=120),
        show(proxies))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)