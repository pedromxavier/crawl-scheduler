import abc
import requests
import pickle
import os
import re

import threading as th

from progress import Progress


class BaseCrawler:

    MAX_THREADS = 30

    def __init__(self, *args, fname: str=None, **kwargs):
        self.fname: str = fname
        self._data: list = []
        self._miss: list = []

        self._ready: bool = False

        if self.fname is not None and os.path.exists(self.crlfname): self.load()

    @classmethod
    def request(cls, url: str, **params: dict) -> str:
        return str(requests.get(url, params).content)

    def _crawl_this(self, url: str, *args: tuple):
        try:
            html_text: str = self.request(url)
        except requests.exceptions.ConnectionError:
            with self.miss_lock: self._miss.append((url, *args))
            self.prog.fault()
            return
        finally:
            self.smph.release()

        try:
            ## Process Results
            results: list = self.crawl(html_text, *args)
        except Exception:
            with self.miss_lock: self._miss.append((url, *args))
            self.prog.fault()
            return
        else:
            if results is not None:
                with self.data_lock: self._data.extend(results)
            next(self.prog)

    def _crawl(self, data: list) -> list:
        """ Retrieves and stores results from crawling over
            `url` response in `self._data`
        """
        

        self.smph = th.Semaphore(self.MAX_THREADS)
        self.miss_lock = th.Lock()
        self.data_lock = th.Lock()

        total = len(data)

        with Progress(total) as self.prog:
            for url, *args in data:
                self.smph.acquire()
                new_thread = th.Thread(target=self._crawl_this, args=(url, *args))
                new_thread.start()
            self.prog.wait()

        return self._miss

    @abc.abstractclassmethod
    def crawl(cls, html_text: str, *args: tuple) -> list:
        """ Crawler.crawl(html_text: str, *args: tuple) -> list
            Returns results from crawling over `html_text` with
            parameters `args`.
        """
        raise NotImplementedError

    def __rshift__(self, other):
        if other._ready: return other

        if isinstance(other, BaseScheduler):
            other._schedule(self.data)
        elif isinstance(other, type(self)):
            raise NotImplementedError
        else:
            raise TypeError
        return other

    @property
    def data(self):
        return [item for item in self._data if item is not None]

    @property
    def crlfname(self):
        return f"{self.fname}.crl"

    def save(self):
        if not os.path.exists(self.crlfname): self.dump()

    def dump(self):
        with open(self.crlfname, 'wb') as file:
            pickle.dump(self._data, file)

    def load(self):
        with open(self.crlfname, 'rb') as file:
            self._data: dict = pickle.load(file)
        print(f"<{self.crlfname}> loaded.")
        self._ready = True

class BaseScheduler:

    def __init__(self, *args, fname: str=None, **kwargs):
        self.fname: str = fname
        self._data: list = []
        self._ready: bool = False

        if self.fname is not None and os.path.exists(self.schfname): self.load()

    def _schedule(self, data: list, *args: tuple):
        self._data.extend(self.schedule(data, *args))
        self._ready = True

    @abc.abstractclassmethod
    def schedule(cls, data: list, *args: tuple):
        raise NotImplementedError

    def __rshift__(self, other):
        if other._ready: return other

        if not self._ready: self._schedule([])

        if isinstance(other, BaseCrawler):
            missing = other._crawl(self.data)
        elif isinstance(other, type(self)):
            raise NotImplementedError
        else:
            raise TypeError

        self._data = missing

        return other

    @property
    def data(self):
        return [item for item in self._data if item is not None]

    @property
    def schfname(self):
        return f"{self.fname}.sch"

    def save(self):
        if not os.path.exists(self.schfname): self.dump()

    def dump(self):
        with open(self.schfname, 'wb') as file:
            pickle.dump(self._data, file)

    def load(self):
        with open(self.schfname, 'rb') as file:
            self._data: dict = pickle.load(file)
        print(f"<{self.schfname}> loaded.")
        self._ready = True

