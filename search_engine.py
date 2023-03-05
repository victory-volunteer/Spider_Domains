# -- coding: utf-8 --
import os
import argparse
import math
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import threading
import time, re, random
import asyncio, aiofiles, aiohttp

import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

HEADERS = {
    "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)"
}

PROXY = [

]


async def produce(queue, read_file_name):
    async with aiofiles.open(read_file_name, mode="r", encoding="utf-8") as fp:
        count = 0
        async for line in fp:
            count += 1
            source_domain = line.strip()

            await queue.put(source_domain)

    print(f"本次读取: {count}行")


async def bing_data_deal(text, write_file_name, lock):
    obj1 = re.compile(r'<cite>(.*?)</cite>')
    obj2 = re.compile(r'<.*?>')
    obj3 = re.compile(r'(http(s)?://)?([^/]*)')
    obj4 = re.compile(r'.*?//')
    links = obj1.finditer(text)
    for link in links:
        data = re.sub(obj2, '', link.group(1))
        data = obj3.search(data)
        data = re.sub(obj4, '', data.group())

        lock.acquire()
        with open(write_file_name, mode="a", encoding="utf-8") as fp:
            fp.write(f"{data}\n")
        lock.release()


async def bing(url, write_file_name, proxy, lock):
    # print(url)  # 仅做测试
    async with aiohttp.ClientSession() as session:
        try:
            await asyncio.sleep(random.uniform(0.1, 0.8))
            # async with session.get(url, headers=HEADERS, timeout=5) as resp:
            async with session.get(url, headers=HEADERS, timeout=5, proxy=proxy) as resp:
                text = await resp.text()
                obj0 = re.compile(r'<li class="b_no"><h1>没有与此相关的结果:')
                if obj0.search(text) is None:
                    await bing_data_deal(text, write_file_name, lock)
                    resp.close()
                    for i in range(1, 4):  # 在第一页的基础上再请求3页
                        url1 = url + f"&first={i * 9}"
                        await asyncio.sleep(random.uniform(0.1, 0.8))
                        # 这里不使用异常捕获更好, 出现异常后全部交给外面的异常捕获处理, 将出错的大url直接写入错误文件
                        # async with session.get(url1, headers=HEADERS, timeout=5) as resp:
                        async with session.get(url1, headers=HEADERS, timeout=5, proxy=proxy) as resp:
                            text = await resp.text()
                            await bing_data_deal(text, write_file_name, lock)
                            resp.close()
                else:
                    resp.close()
        except Exception as result:
            # print(f'bing出错: {url}\n错误原因: {result}')  # 仅做测试
            return 1


async def baidu_data_deal(text, write_file_name, lock):
    obj1 = re.compile(r'<div class=.*?srcid=.*?id=.*?tpl=.*?mu="(.*?)"', re.S)
    obj2 = re.compile(r'(http(s)?://)?([^/]*)')
    obj3 = re.compile(r'.*?//')
    links = obj1.finditer(text)
    for i, link in enumerate(links):
        if i == 0:
            continue
        data1 = link.group(1)
        data2 = obj2.search(data1)
        data = re.sub(obj3, '', data2.group())
        lock.acquire()
        with open(write_file_name, mode="a", encoding="utf-8") as fp:
            fp.write(f"{data}\n")
        lock.release()


async def baidu(url, write_file_name, proxy, lock):
    # print(url)  # 仅做测试
    async with aiohttp.ClientSession() as session:
        try:
            await asyncio.sleep(random.uniform(0.1, 0.8))
            # async with session.get(url, headers=HEADERS, timeout=5) as resp:
            async with session.get(url, headers=HEADERS, timeout=5, proxy=proxy) as resp:
                text = await resp.text()
                obj4 = re.compile(r'<div class="timeout-feedback-icon"></div>')
                obj0 = re.compile(r'<div class="nors"><p>抱歉没有找到')
                if obj4.search(text) is not None:
                    resp.close()
                    ex = Exception(f"{proxy}被百度封禁")
                    raise ex
                elif obj0.search(text) is None:
                    await baidu_data_deal(text, write_file_name, lock)
                    resp.close()
                    for i in range(1, 4):  # 在第一页的基础上再请求3页
                        url1 = url + f"&pn={i * 10}"
                        await asyncio.sleep(random.uniform(0.1, 0.8))
                        # async with session.get(url1, headers=HEADERS, timeout=5) as resp:
                        async with session.get(url1, headers=HEADERS, timeout=5, proxy=proxy) as resp:
                            text = await resp.text()
                            if obj4.search(text) is not None:
                                resp.close()
                                ex = Exception(f"{proxy}被百度封禁")
                                raise ex
                            await baidu_data_deal(text, write_file_name, lock)
                            resp.close()
                else:
                    resp.close()
        except Exception as result:
            # print(f'baidu出错: {url}\n错误原因: {result}')  # 仅做测试
            return 1


async def url_deal(source_domain, write_file_name, error_file_name, lock):
    """添加其他搜索引擎只在这里做处理"""
    # 对于同一个source_domain来说, 个人认为使用同一个代理会更好, 因为同一个代理的话, 要能访问就都能访问, 要不能访问就都不能访问, 减少差异
    ip = random.choice(PROXY)
    proxy = f""

    # bing引擎处理
    bing_url = f"http://cn.bing.com/search?q=domain:{source_domain}"
    result_bing = await bing(bing_url, write_file_name, proxy, lock)

    # 返回值是None说明没有发生异常, 如果发生异常就不再执行该source_domain的后续所有处理, 直接将其写入错误文件
    if result_bing is None:
        # baidu引擎处理
        baidu_url = f"http://www.baidu.com/s?wd=site:{source_domain}"
        result_baidu = await baidu(baidu_url, write_file_name, proxy, lock)
        if result_baidu is not None:
            # 出错原因: 网络延迟, ip失效, ip被封(百度有安全检测)
            # print(f"baidu引擎:{source_domain}访问超时, 错误ip:{ip}")
            lock.acquire()
            with open(error_file_name, mode="a", encoding="utf-8") as fp:
                fp.write(f"{source_domain}\n")
            lock.release()
    else:
        # 出错原因: 网络延迟, ip失效
        # print(f"bing引擎:{source_domain}访问超时, 错误ip:{ip}")
        lock.acquire()
        with open(error_file_name, mode="a", encoding="utf-8") as fp:
            fp.write(f"{source_domain}\n")
        lock.release()


async def consume(queue, write_file_name, error_file_name, lock):
    while True:
        source_domain = await queue.get()
        await url_deal(source_domain, write_file_name, error_file_name, lock)
        queue.task_done()


async def run(read_file_name, write_file_name, error_file_name, lock):
    queue = asyncio.Queue(maxsize=1000)  # 可以不限制
    tasks = [asyncio.create_task(consume(queue, write_file_name, error_file_name, lock)) for _ in range(500)]
    await produce(queue, read_file_name)
    await queue.join()
    for c in tasks:
        c.cancel()


def linux_order(filename):
    # 计算进程和线程数
    # 每个核起一个进程, 每个进程起5倍cpu核数量个线程
    result1 = os.popen('cat /proc/cpuinfo | grep "cores" | uniq')
    process_count = int(result1.read().strip().rsplit(" ", 1)[1])
    process_thread_count = process_count * 5
    thread_count = process_thread_count * process_count
    print(f"进程总数{process_count}, 线程总数{thread_count}")

    # 根据线程数拆分文件
    result = os.popen(f'wc -l {filename}')
    line = result.read().split(" ", 1)[0]
    count = math.ceil(int(line) / thread_count)
    print(f"文件总行数: {line}, 每{count}行拆分一次")
    status = os.system(f'split -l {count} -d {filename} part_')
    print(f"拆分状态码: {status}")

    # 取拆分后的文件个数
    result2 = os.popen('ls | grep part_ | wc -l')
    file_count = int(result2.read().strip())
    print(f"拆分后文件个数: {file_count}")

    return process_count, process_thread_count, file_count


def process_work(process_thread_count, filename_queue, write_file_id):
    lock = threading.Lock()  # 千万不能往进程池里传, 必须传入线程池
    with ThreadPoolExecutor(process_thread_count) as t:
        for _ in range(process_thread_count):
            t.submit(thread_work, filename_queue, write_file_id, lock)


def thread_work(filename_queue, write_file_id, lock):
    while True:
        item = filename_queue.get()
        if item is None:
            # None是停止的信号
            filename_queue.task_done()
            break
        else:
            # print(item)
            coroutines_work(item, write_file_id, lock)
            filename_queue.task_done()


def coroutines_work(filename, write_file_id, lock):
    # dns文件路径
    read_file_name = f"./{filename}"
    # 数据文件路径
    write_file_name = f"./data_{write_file_id}.txt"
    # 错误dns文件路径
    error_file_name = f"./error_data_{write_file_id}.txt"

    # linux或mac必须这样写(window这样写有可能会报错, 因为asyncio对window支持不好)
    asyncio.run(run(read_file_name, write_file_name, error_file_name, lock))


def split_filename_queue(filename_queue, process_count, process_thread_count, file_count):
    # 1090个以内的文件名可控(一般最多是8核cpu, 最大线程数=8*5*8=320, 在此处拆分的最大文件个数也才320个)
    for i in range(file_count):
        if i < 90:
            name = "part_%02d" % i
            filename_queue.put(name)
        else:
            name = "part_9%03d" % (i - 90)
            filename_queue.put(name)
    for i in range(process_count * process_thread_count):
        filename_queue.put(None)


def main(filename):
    process_count, process_thread_count, file_count = linux_order(filename)

    filename_queue = multiprocessing.Manager().Queue()

    split_filename_queue(filename_queue, process_count, process_thread_count, file_count)

    with ProcessPoolExecutor(process_count) as t:
        for write_file_id in range(process_count):  # 思路: 在进程池中把i作为标记, 使每个进程写入不同的文件, 这样就不需要加进程锁了
            t.submit(process_work, process_thread_count, filename_queue, write_file_id)


if __name__ == '__main__':
    start = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', type=str, default='')
    args = parser.parse_args()
    if args.filename == '':
        print("传参格式: --filename 文件名")
    else:
        main(args.filename)

    end = time.time()
    print(end - start)