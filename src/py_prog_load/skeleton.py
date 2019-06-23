#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
[options.entry_points] section in setup.cfg:

    console_scripts =
         fibonacci = py_prog_load.skeleton:run

Then run `python setup.py install` which will install the command `fibonacci`
inside your current environment.
Besides console scripts, the header (i.e. until _logger...) of this file can
also be used as template for Python modules.

Note: This skeleton file can be safely removed if not needed!
"""

import argparse
import sys
import logging
import os

from functools import partial
from hashlib import md5
import io

try:
    from multiprocessing.dummy import Pool as ThreadPool
except ImportError:
    pass

from typing import BinaryIO, Callable, Dict, List, Tuple, TypeVar, Union
from threading import Lock

from py_prog_load import __version__

__author__ = "David Longo"
__copyright__ = "David Longo"
__license__ = "mit"

logging.basicConfig(format="%(threadName)s:%(message)s")
_logger = logging.getLogger(__name__)


class ProgLoader:
    _paths = None
    _pool: ThreadPool = None

    _tmp = '/Users/icarus/tmp/%s-prog-loader-' % str(os.getpid())

    """
    Constructors
        :param self: 
        :param paths: 
        :param *args: 
        :param **kwargs:
    """

    def __init__(self, paths, *args, **kwargs):
        self._paths = paths
        try:
            self._pool: ThreadPool = ThreadPool()
        except:
            self._pool: ThreadPool = None
        if(kwargs.get("tmp") is not None):
            self._tmp = kwargs.get("tmp")

        return

    @classmethod
    def pre_process(cls,
            f: Callable[
                [Tuple[str, TypeVar]],
                Union[List[str], str]],
            *args, **kwargs):  # ->int
        setup_logging(logging.DEBUG)
        self = cls(kwargs.get("paths"), kwargs)
        pathsList = self._paths
        count_dict: Dict = None
        l_count_dict: Lock = Lock()
        result = dict()
        l_result = Lock()
        chunkSet = kwargs.get("chunkSet")
        chunkSet = chunkSet if chunkSet else ['done']
        count_dict = dict(((chunk, 0) for chunk in chunkSet))

        """
        pp_thread
        :param record: 
        :param f
            Callable function taking (label:str, data:TypeVar) as params
            returns file path(s)
        :param Union[str: 
        :param bytes]: 
        """
        def pp_thread(source, *args, **kwargs):  # ->bool
            self = kwargs.get("self")
            """
            Thread Function
            """
            count_dict, l_count_dict, result, l_result = (
                kwargs.get("count_dict"),
                kwargs.get("l_count_dict"),
                kwargs.get("result"),
                kwargs.get("l_result"),
            )

            try:
                """
                Source is any input string or IO object
                Example: A source image
                """
                tmpFiles = f(source)
                if type(tmpFiles) is not list:
                    tmpFiles = [tmpFiles]

                """
                tmpFiles now contains a List of resulting transformations
                Example: Image rotations
                """

                for target in tmpFiles:
                    #print("\t%s" % str(target)) #TODO: Logging
                    try:
                        label, data = target
                    except ValueError:
                        data = target
                        label = 'done'

                    if type(data) is not str:
                        data = str(data)

                    with l_count_dict:
                        try:
                            count_dict[label] += 1
                        except KeyError:
                            count_dict[label] = 1
                    codestr = label + str(count_dict[label])
                    #print("{} {} = {}".format(
                    #    label, count_dict[label], codestr)) #TODO: Logging
                    labelHash = md5(
                        codestr.encode()).hexdigest() + \
                            "-%s" % count_dict[label]
                    targetPath = self._tmp + labelHash

                    # Try establishing a new file
                    writeMode = "x"
                    existingFile = None
                    try:
                        file = open(targetPath, writeMode)
                        _logger.debug("Trying to write to %s" %
                                      str(targetPath))
                    except FileExistsError:
                        """
                        If one exists, check that we haven't already 
                        preprocessed it
                        """
                        existingFile = open(targetPath, "r")
                        existingContents = existingFile.read()

                        if(md5(existingContents.encode()).digest() ==
                                md5(data.encode()).digest()):
                            with l_result:
                                # Keep track of resulting files
                                result[targetPath] = label
                            return True
                        else:
                            writeMode = "w"
                    except:
                        raise
                    finally:
                        if existingFile is not None:
                            existingFile.close()

                    # If the file is erroneous, overwrite it
                    if writeMode is "w":
                        try:
                            file = open(targetPath, "w")
                        except OSError as e:
                            _logger.error(
                                "Tmp disk out of space: %s". sys.exc_info()[0])
                            raise
                        except:
                            raise

                    try:
                        file.write(data)
                    finally:
                        file.close()

                    with l_result:
                        # Keep track of resulting files
                        result[targetPath] = label
                        print(result)
            except:
                _logger.error("Unexpected error: %s", sys.exc_info()[0])
                print("Unexpected error: %s" % sys.exc_info()[0])
                raise
                return False
            return True
        # end of pp_thread function

        try:
            pathsList = list(pathsList.items())
        except AttributeError:
            pass

        if self._pool is not None:
            try:
                _ = [self._pool.map(
                    partial(pp_thread,
                        f=f, count_dict=count_dict, l_count_dict=l_count_dict,
                        result=result, l_result=l_result, self=self),
                    (path for path in pathsList))]
            except:
                raise  
        else:
            _ = list(map(partial(pp_thread,
                        f=f, count_dict=count_dict, l_count_dict=l_count_dict,
                        result=result, l_result=l_result, self=self),
                        (path for path in pathsList)))

        self._paths = result

        return self

    def save(self, targetPath):
        l_paths:Lock = Lock()
        
        try: pathList = list(self._paths.items())
        except: pass
        
        def saveThread(targetPath, tmp, paths, l_paths, target):
            path = target[0]
            os.rename(path, targetPath + path[len(self._tmp):])
            with l_paths:
                del target
    
        try:
            _ = [self._pool.map(partial(saveThread,
                                    targetPath, self._tmp, 
                                    self._paths, l_paths),
                    (target for target in pathList))]
        except: raise
        return

    def cleanup(self):
        def rem(path):
            os.remove(path)
        pathsList = list(self._paths.items())

        with self._pool:
            _ = [self._pool.map(rem, (path[0] for path in pathsList))]

        # TODO: Logging

    def _wait(self):
        if self._pool is not None:
            try:
                self._pool.close()
                self._pool.join()
            except:
                # TODO: Add exception handling
                raise
        return

    def __del__(self):
        if self._pool is not None:
            self._wait()

        return

    """
    Attributes
    """

    def __str__(self):
        if self._dataType == 0:
            return str(self._paths)
        elif self._dataType == 1:
            return str(self._pathsDict)
        else:
            return "Empty"
        return


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Just a Fibonnaci demonstration")
    parser.add_argument(
        '--version',
        action='version',
        version='py_prog_load {ver}'.format(ver=__version__))
    parser.add_argument(
        dest="n",
        help="n-th Fibonacci number",
        type=int,
        metavar="INT")
    parser.add_argument(
        '-v',
        '--verbose',
        dest="loglevel",
        help="set loglevel to INFO",
        action='store_const',
        const=logging.INFO)
    parser.add_argument(
        '-vv',
        '--very-verbose',
        dest="loglevel",
        help="set loglevel to DEBUG",
        action='store_const',
        const=logging.DEBUG)
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    """
    args = parse_args(args)
    """
    setup_logging(logging.DEBUG)  # args.loglevel)
    #_logger.debug("Starting crazy calculations...")
    #print("The {}-th Fibonacci number is {}".format(args.n, fib(args.n)))
    #_logger.info("Script ends here")

    from typing import List, Tuple, TypeVar

    myDict = {
        "golden": 1,
        "shepherd": 47
    }

    def testFunc(record: Tuple[str, TypeVar])->List:
        label, data = record
        result = list()
        for i in range(0, 25, 5):
            result.append((label, data + i))
        return result

    loader = ProgLoader.pre_process(f=testFunc, paths=list(
        myDict.items()), tmp="/Users/icarus/tmp/")
    #print("Preprocessed %d files" % len(loader._paths))
    # TODO: Logging

    loader.save("/Users/icarus/tmp2/")
    #loader.cleanup()

    print(loader._paths)


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
