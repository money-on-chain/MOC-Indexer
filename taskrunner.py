import abc
import datetime
import inspect
import logging.config
import os
import sys
import threading
from abc import abstractmethod
from weakref import WeakValueDictionary

import boto3 as boto3
import pymongo
from moneyonchain.manager import ConnectionManager
from timeloop import Timeloop
from timeloop.job import Job as TLJob

from moc_config import MoCCfg


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')



class RetriableException(Exception):
    retryInSeconds = 60


class Job(TLJob, abc.ABC):
    IntervalSeconds = 20
    Name = None

    def __init__(self, runner):
        super().__init__(self.IntervalSeconds, self.looprun)
        self.runner = runner
        self.con = ConnectionManager(options=self.options, network=self.network)
        self.initialized = False
        self.logger = logging.getLogger(self.name)

    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            if self.stopped.is_set(): break
            self.execute(*self.args, **self.kwargs)

    @property
    def options(self):
        return self.runner.options

    @property
    def network(self):
        return self.runner.network

    @property
    def name(self):
        try:
            ret = self.Name
        except:
            ret = None
        if ret is None:
            ret = self.__class__.__name__
        return ret

    @property
    def Interval(self):
        return datetime.timedelta(seconds=self.IntervalSeconds)

    def _set_interval(self, seconds):
        self._interval = seconds

    def _get_interval(self):
        return datetime.timedelta(seconds=self._interval)

    interval = property(_get_interval, _set_interval)

    def __init(self):
        if not self.initialized:
            self.initialized = True
            self.plugin_init()

    @classmethod
    def IsJob(cls):
        return True

    def looprun(self):
        try:
            self.__init()
        except Exception as err:
            logging.getLogger('default').error(err)
            self.runner.stop()
            self.stopped.set()
            return

        try:
            self.run_job()
        except Exception as e:
            log.error(e, exc_info=True)
            self.runner.aws_put_metric_heart_beat(1)

    def plugin_init(self):
        pass

    @property
    def block_number(self):
        return Blockchain.GetBlockNr(self.con)

    def getBlock(self, nr):
        return Blockchain.GetBlock(self.con, nr)

    @abstractmethod
    def run_job(self):
        raise NotImplementedError()

    @classmethod
    def Run(cls, _config, _network):
        class FakeRunner:
            options = _config
            network = _network
        return cls.RunWithRunner(FakeRunner())

    @classmethod
    def RunWithRunner(cls, runner):
        return cls(runner)


class Blockchain:
    BlocksLock = threading.RLock()
    Keep = 20
    Blocks = {}
    # BlockNumberLock = threading.RLock()
    # BlockNumberTS = None
    # BlockNumber = None
    # BlockNumberUpdate = 1

    @classmethod
    def Reset(cls):
        with cls.BlocksLock:
            cls.Blocks = {}
        # with cls.BlockNumberLock:
        #     cls.BlockNumberTS = None
        #     cls.BlockNumber = None
        #     cls.BlockNumberUpdate = 1

    @classmethod
    def GetBlock(cls, con, nr, useCache=True):
        if nr!='latest' and useCache:
            with cls.BlocksLock:
                block = cls.Blocks.get(nr)
        else:
            block = None

        if block is None:
            block = con.get_block(nr, full_transactions=True)
            if nr=='latest':
                nr = block['number']
            with cls.BlocksLock:
                cls.Blocks[nr] = block
                cls.PurgeOlder()
        return block

    @classmethod
    def PurgeOlder(cls):
        with cls.BlocksLock:
            keys = list(cls.Blocks.keys())
            keys.sort()
            remove = keys[:-cls.Keep]
            for key in remove:
                cls.Blocks.pop(key)

    @classmethod
    def GetBlockNr(cls, con):
        # with cls.BlockNumberLock:
        #     ts = time.time()
        #     if True:  # (None in (cls.BlockNumberTS, cls.BlockNumber)) or (
        #         # cls.BlockNumberTS+cls.BlockNumberUpdate<ts):
        #         cls.BlockNumber = con.block_number
        #         cls.BlockNumberTS = ts
        #     return cls.BlockNumber
        return con.block_number

    def __init__(self, con):
        self.con = con

    @property
    def block_number(self):
        return self.GetBlockNr(self.con)

    def getBlock(self, nr, useCache=True):
        return self.GetBlock(self.con, nr, useCache=useCache)

    def getTxReceipt(self, txhash):
        return self.con.get_transaction_receipt(txhash)


class BlockBasedJob(Job, abc.ABC):
    # noinspection PyAttributeOutsideInit
    def plugin_init(self):
        self.prev = None
        super().plugin_init()

    def run_job(self):
        blocknr = self.block_number
        if (self.prev is None):
            block = self.getBlock(blocknr)
            self.on_new_block(block)
            self.prev = block
            return

        while self.prev['number'] < blocknr and not self.stopped.is_set():
            self.logger.warning("will catch up to : %d" % blocknr)
            block = self.getBlock(self.prev['number'] + 1)
            self.on_new_block(block)
            self.prev = block

    @abstractmethod
    def on_new_block(self, block):
        raise NotImplementedError()


class ConfirmedBlockBasedJob(BlockBasedJob, abc.ABC):
    Confirmations = 12
    Retry = 5

    def on_new_block(self, block):
        blocknr = block['number'] - self.Confirmations
        while False == self.on_confirmed_block(blocknr):
            self.logger.warning(
                "Confirmation of %d failed, will retry.." % blocknr)
            if self.stopped.wait(self.Retry):
                break

    @abc.abstractmethod
    def on_confirmed_block(self, block):
        raise NotImplementedError()


class TimeloopExt(Timeloop):
    def __init__(self):
        super().__init__()
        # override logger to prevent duplicated logging
        self.logger = logging.getLogger('default')
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        self.logger.setLevel(logging.INFO)

    def _my_add_job(self, job):
        self.jobs.append(job)

    def _start_jobs(self, block):
        for j in self.jobs:
            j.daemon = not block
            j.start()
            self.logger.info("Registered job {}".format(j))

    def _stop_jobs(self):
        L = len(self.jobs)
        for idx, j in enumerate(self.jobs):
            self.logger.info("Stopping job %d/%d: %s" % (idx + 1, L, j))
            j.stop()


class JobsRunner:
    def __init__(self, **kwargs):
        self.moccfg = moccfg = kwargs['moccfg']
        self.cfg = kwargs
        self.logger = logging.getLogger("jobs-runner")
        self.options = moccfg.config
        self.network = moccfg.network
        self.tl = TimeloopExt()
        self._db = None
        self._client = None
        self._dbname = os.environ.get('APP_MONGO_DB', self.options['mongo']['db'])
        self._dburi = os.environ.get('APP_MONGO_URI', self.options['mongo']['uri'])
        self.quitEvent = None
        self.stopped = None
        self._adding_jobs_msg = False
        self._modules = WeakValueDictionary()

    def connect(self):
        return self.client

    @property
    def dbname(self):
        return self._dbname

    @property
    def dburi(self):
        return self._dburi

    @property
    def client(self):
        if self._client is None:
            self._client = pymongo.MongoClient(self.dburi)
        return self._client

    @property
    def db(self):
        return self.client[self.dbname]

    @staticmethod
    def aws_put_metric_heart_beat(value):
        if 'AWS_ACCESS_KEY_ID' not in os.environ:
            return
        # Create CloudWatch client
        cloudwatch = boto3.client('cloudwatch')
        # Put custom metrics
        cloudwatch.put_metric_data(
                MetricData=[{
                    'MetricName': os.environ['MOC_INDEXER_NAME'],
                    'Dimensions': [{
                        'Name': 'INDEXER',
                        'Value': 'Error'
                    }, ],
                    'Unit': 'None',
                    'Value': value
                }, ],
                Namespace='MOC/EXCEPTIONS')

    def add_jobs(self, joblist):
        if not self._adding_jobs_msg:
            self._adding_jobs_msg = True
            log.info("Starting adding jobs...")
            self.aws_put_metric_heart_beat(0)

        for jobklass in joblist:
            job = jobklass(self)
            log.info("Jobs add: %s" % job.name)
            self.tl._my_add_job(job)

    def add_jobdesc(self, job_desc):
        modulepath, classname = job_desc.split(":")
        mod = self._modules.get(modulepath)
        if mod is None:
            mod = __import__(modulepath)
            self._modules[modulepath] = mod
        # take submodules and append to klass-name
        submods = modulepath.split('.')[1:]
        while submods:
            subname = submods.pop(0)
            mod = getattr(mod, subname)
        self._load_class(mod, classname)

    def _load_class(self, mod, classname):
        if not classname=='*':
            jobklass = getattr(mod, classname)
            return self.add_jobs([jobklass])
        try:
            class_list = getattr(mod, '__all__')
        except AttributeError:
            class_list = dir(mod)
        for attrname in class_list:
            attr = getattr(mod, attrname)
            if inspect.isclass(attr) and (not inspect.isabstract(attr)):
                if issubclass(attr, (Job,)) or (hasattr(attr, 'IsJob') and
                    attr.IsJob()):
                    self._load_class(mod, attrname)

    def time_loop_start(self):
        return self.start(block=True)

    def start(self, block=False):
        self.quitEvent = threading.Event()
        self.stopped = threading.Event()
        self.tl.start(block=False)
        if block:
            return self.wait_and_stop()
        tr = threading.Thread(target=self.wait_and_stop)
        tr.start()

    def wait_and_stop(self):
        try:
            # we don't directly wait to allow keyboard interrupt to stop this
            # process..
            while not self.quitEvent.is_set():
                self.quitEvent.wait(1)
        except KeyboardInterrupt:
            log.info("Shutting DOWN! TASKS")
        self.tl.stop()
        self.stopped.set()

    def stop(self):
        if self.quitEvent is None:
            return self.logger.warning("Trying to stop but not running.")
        if self.quitEvent.is_set():
            return self.logger.warning("Trying to stop already stopped.")
        self.quitEvent.set()
        return self.stopped

def extract_plugins_from_cmd(sys_argv, offset=1):
    plugins = [arg for arg in sys_argv[offset:] if ':' in arg]
    for plugin in plugins:
        sys_argv.remove(plugin)
    return plugins

def main(prog='taskrunner.py', plugins=None):
    if plugins is None:
        plugins = extract_plugins_from_cmd(sys.argv, 1)
    moccfg = MoCCfg(prog=prog)
    runner = JobsRunner(moccfg=moccfg)
    for plugin in plugins:
        runner.add_jobdesc(plugin)
    runner.time_loop_start()


if __name__ == '__main__':
    main()
