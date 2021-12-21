import asyncio
import time
import pandas as pd
import jdatetime as jd
from datetime import timedelta
from tqdm import tqdm

from requests_html import HTMLSession, AsyncHTMLSession

# number of workers to run in parallel
WORKERS = 64
# number of pages to scrap
THRESHOLD = 5
# default URL template
URL_TEMPLATE = 'https://fidibo.com/category/literature/short-story?page='
DATE_FORMAT = '%Y/%m/%d'
HEADERS = {'X-Requested-With': 'XMLHttpRequest'}

session = HTMLSession()
session.headers.update(HEADERS)  # send HEADERS with all requests
r = session.get('https://fidibo.com/category/literature/short-story')

# get number of pages available
TOTAL_PAGES = r.json()['total_pages']
SIZE_PER_PAGE = r.json()['size']

# books that are going to be scraped
TOTAL_BOOKS = SIZE_PER_PAGE * THRESHOLD

# choose smaller between total_pages and threshold for threshold value
THRESHOLD = min(THRESHOLD, TOTAL_PAGES)

print(f'[i] Number of workers {WORKERS}')
print(f'[i] Total pages is {TOTAL_PAGES}')
print(f'[i] Number of pages to be scrapped {THRESHOLD}')

# dataset columns
all_books = []
all_comments = []


async def worker(job_queue: asyncio.Queue, response_queue: asyncio.Queue, pbar):
    """
    this function run asynchronously and GET the book pages
    of the provided URL and put() the Response object
    to the responses_queue.
    """
    while True:
        # get() the page number of the page to scrap
        page_number = await job_queue.get()
        # print(f'[i] getting page {page_number}')
        # create an session object which runs asynchronously
        asession = AsyncHTMLSession()
        asession.headers.update(HEADERS)
        response = await asession.get(URL_TEMPLATE + page_number)
        response_queue.put_nowait(response)
        job_queue.task_done()

        # update progress bar
        pbar.update(1)


async def comment_worker(job_queue: asyncio.Queue, response_queue: asyncio.Queue, p_bar):
    """
    this function run asynchronously and GET the comments of books
    of the provided URL and put() the Response object
    to the responses_queue.
    """
    while True:
        # get() the page number of the page to scrap
        data = await job_queue.get()
        # print(f'[i] getting page {page_number}')
        # create an session object which runs asynchronously
        asession = AsyncHTMLSession()
        asession.headers.update(HEADERS)
        response = await asession.post('https://fidibo.com/book/comment', data)
        response_queue.put_nowait((response, data))
        job_queue.task_done()
        # print(f'[i] got page {page_number}')

        # update progress bar
        p_bar.update(1)


async def parse(res_q: asyncio.Queue, comments_job: asyncio.Queue, book_q):
    """
    this function get book attributes and queue job for
    getting comment total pages to be scraped and then
    create jobs for comments to be scraped
    """
    job_queue = asyncio.Queue()
    response_queue = asyncio.Queue()

    # while the is still a result available in the Queue
    while not res_q.empty():
        res = await res_q.get()
        data = res.json()
        [
            book_q.put_nowait({'id': _['id'], 'title': _['title'], 'authors': _['authors'], 'url': _['url']})
            for _ in data['books']
        ]

        for _ in data['books']:
            book_id = _['id']
            page = 1
            post_data = {'book_id': book_id, 'page': page}

            job_queue.put_nowait(post_data)

    pbar = tqdm(total=job_queue.qsize())
    tasks = []
    for i in range(WORKERS):
        task = asyncio.create_task(comment_worker(job_queue, response_queue, pbar))
        tasks.append(task)

    # for progress bar
    await job_queue.join()
    pbar.close()

    # create jobs for comments to be scraped
    while not response_queue.empty():
        response, post_data = response_queue.get_nowait()
        total_comments_page = response.json()['comment_paging']['pageCount']

        for page in range(1, total_comments_page + 1):
            post_data['page'] = page
            await comments_job.put(post_data)

    for task in tasks:
        task.cancel()


async def get_books_comments(comments_res: asyncio.Queue, comments_q: asyncio.Queue):
    """
    create comment dict from previous results
    """
    while not comments_res.empty():
        cms, post_data = comments_res.get_nowait()
        comments = cms.json()
        for comment in comments.get('Comments'):
            comments_q.put_nowait({
                'id': comment.get('id'),
                'book_id': comment.get('book_id'),
                'comment': comment.get('comment')
            })


async def main():
    # create FIFO Queue for both jobs to be done
    # and the result of the jobs
    job_queue = asyncio.Queue()
    response_queue = asyncio.Queue()
    book_queue = asyncio.Queue()

    comments_jobs = asyncio.Queue()
    comments_response = asyncio.Queue()
    comments_queue = asyncio.Queue()

    # create all page numbers that need to be scraped
    for _ in range(THRESHOLD):
        job_queue.put_nowait(str(_ + 1))  # to start from 1

    pbar = tqdm(total=THRESHOLD)
    # Create WORKER number of worker tasks to process the queue concurrently.
    tasks = []
    for i in range(WORKERS):
        task = asyncio.create_task(worker(job_queue, response_queue, pbar))
        tasks.append(task)

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await job_queue.join()
    total_slept_for = time.monotonic() - started_at
    # close progressbar for the worker()
    pbar.close()

    print('\n[i] getting book URLs...')
    await parse(response_queue, comments_jobs, book_queue)
    print('[i] got all book URLs.')

    while not book_queue.empty():
        all_books.append(book_queue.get_nowait())

    # run all jobs for comments of books to be scraped
    comment_pbar = tqdm(total=comments_jobs.qsize())
    comment_tasks = []
    for i in range(WORKERS):
        task = asyncio.create_task(comment_worker(comments_jobs, comments_response, comment_pbar))
        comment_tasks.append(task)

    await comments_jobs.join()
    comment_pbar.close()

    print('\n[i] getting book comments...')
    await get_books_comments(comments_response, comments_queue)
    print('[i] got all book comments.')

    while not comments_queue.empty():
        all_comments.append(comments_queue.get_nowait())

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()

    for task in comment_tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*comment_tasks, return_exceptions=True)


asyncio.run(main())

# create dataframe and save them
df = pd.DataFrame(all_comments)
df.to_excel('comments.xlsx')
df.to_csv('comments.csv')

df = pd.DataFrame(all_books)
df.to_excel('books.xlsx')
df.to_csv('books.csv')

# saved dataframes will be analysed using score_comments.py
