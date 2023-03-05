# -- coding: utf-8 --
import os
import argparse
import math
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import threading
import time
import asyncio, aiofiles
import dns.resolver

import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

DNS_SERVERS = ['180.76.76.76', '119.29.29.29', '223.5.5.5', '223.6.6.6', '182.254.116.116', '114.114.115.115',
               '114.114.114.114']


async def produce(queue, read_file_name, subnames):
    async with aiofiles.open(read_file_name, mode="r", encoding="utf-8") as fp:
        count = 0
        async for line in fp:
            count += 1
            source_domain = line.strip()
            async with aiofiles.open(subnames, 'r') as f:
                async for i in f:
                    i = i.strip()  # 也可以使用replace来替换换行符\n
                    subdomain = i + '.' + source_domain
                    await queue.put(subdomain)
    print(f"本次读取: {count}行")


async def dns_resolver(domain, res, write_file_name, error_file_name, lock):
    try:
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, res.resolve, domain, 'A', dns.rdataclass.IN, False, None, True, 0, 3.05)
        A = await future
    except dns.resolver.NoNameservers:
        lock.acquire()
        with open(error_file_name, mode="a", encoding="utf-8") as fp:
            fp.write(f"DNS服务器异常: {domain}\n")
        lock.release()
        return
    except dns.resolver.NoAnswer:
        # lock.acquire()
        # with open(error_file_name, mode="a", encoding="utf-8") as fp:
        #     fp.write(f"无响应: {domain}\n")
        # lock.release()
        return
    except dns.resolver.LifetimeTimeout:
        # lock.acquire()
        # with open(error_file_name, mode="a", encoding="utf-8") as fp:
        #     fp.write(f"请求超时: {domain}\n")
        # lock.release()
        return
    except dns.resolver.NXDOMAIN:
        return
    except Exception as e:
        print(f"请求{domain}报错: {e}")
        return
    lock.acquire()
    with open(write_file_name, mode="a", encoding="utf-8") as fp:
        fp.write(f"{domain}\n")
    lock.release()


async def consume(queue, write_file_name, error_file_name, lock, res, subnames):
    while True:
        subdomain = await queue.get()
        await dns_resolver(subdomain, res, write_file_name, error_file_name, lock)
        queue.task_done()


async def run(read_file_name, write_file_name, error_file_name, lock, subnames):
    queue = asyncio.Queue(maxsize=3000)
    res = dns.resolver.Resolver(configure=False)  # configure=False代表不使用系统配置
    res.nameservers = DNS_SERVERS
    tasks = [asyncio.create_task(consume(queue, write_file_name, error_file_name, lock, res, subnames)) for _ in
             range(500)]
    await produce(queue, read_file_name, subnames)
    await queue.join()
    for c in tasks:
        c.cancel()


def linux_order(filename):
    # 计算进程和线程数
    # 每个核起一个进程, 每个进程起5倍cpu核数量个线程
    result1 = os.popen('cat /proc/cpuinfo | grep "cores" | uniq')
    process_count = int(result1.read().strip().rsplit(" ", 1)[1])
    process_thread_count = process_count * 35
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


def process_work(process_thread_count, filename_queue, write_file_id, subnames):
    lock = threading.RLock()  # 千万不能往进程池里传, 必须传入线程池
    with ThreadPoolExecutor(process_thread_count) as t:
        for _ in range(process_thread_count):
            t.submit(thread_work, filename_queue, write_file_id, lock, subnames)


def thread_work(filename_queue, write_file_id, lock, subnames):
    while True:
        item = filename_queue.get()
        if item is None:
            # None是停止的信号
            filename_queue.task_done()
            break
        else:
            # print(item)
            coroutines_work(item, write_file_id, lock, subnames)
            filename_queue.task_done()


def coroutines_work(filename, write_file_id, lock, subnames):
    # dns文件路径
    read_file_name = f"./{filename}"
    # dns解析字典
    subnames = f"./{subnames}"
    # 数据文件路径
    write_file_name = f"./data_{write_file_id}.txt"
    # 错误dns文件路径
    error_file_name = f"./error_data_{write_file_id}.txt"

    # linux或mac必须这样写(window这样写有可能会报错, 因为asyncio对window支持不好)
    asyncio.run(run(read_file_name, write_file_name, error_file_name, lock, subnames))


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


def main(filename, subnames):
    process_count, process_thread_count, file_count = linux_order(filename)
    # process_count, process_thread_count, file_count = 2, 10, 2  # 仅做测试

    filename_queue = multiprocessing.Manager().Queue()

    split_filename_queue(filename_queue, process_count, process_thread_count, file_count)

    with ProcessPoolExecutor(process_count) as t:
        for write_file_id in range(process_count):  # 思路: 在进程池中把i作为标记, 使每个进程写入不同的文件, 这样就不需要加进程锁了
            t.submit(process_work, process_thread_count, filename_queue, write_file_id, subnames)


if __name__ == '__main__':
    start = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', type=str, default='')
    parser.add_argument('--subnames', type=str, default='subnames.txt')
    args = parser.parse_args()
    if args.filename == '':
        print("传参格式: --filename 文件名")
    else:
        main(args.filename, args.subnames)

    end = time.time()
    print(end - start)

# python 测试.py --filename text.txt