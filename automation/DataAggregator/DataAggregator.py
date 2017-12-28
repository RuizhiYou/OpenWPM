from __future__ import absolute_import
from __future__ import print_function
from ..SocketInterface import serversocket
from ..MPLogger import loggingclient
from sqlite3 import (OperationalError, ProgrammingError, IntegrityError)
import sqlite3
import time
import six
from six.moves import range
import boto3
import json
import os
import hashlib

s3 = boto3.client('s3')


def DataAggregator(manager_params, status_queue, commit_batch_size=1000):
    """
     Receives SQL queries from other processes and writes them to the central
     database. Executes queries until being told to die (then it will finish
     work and shut down). This process should never be terminated un-gracefully

     <manager_params> TaskManager configuration parameters
     <status_queue> is a queue connect to the TaskManager used for
        communication
     <commit_batch_size> is the number of execution statements that should be
        made before a commit (used for speedup)
    """

    # sets up DB connection
    db_path = manager_params['database_name']
    db = sqlite3.connect(db_path, check_same_thread=False)
    curr = db.cursor()

    # sets up logging connection
    logger = loggingclient(*manager_params['logger_address'])

    # sets up the serversocket to start accepting connections
    sock = serversocket()
    status_queue.put(sock.sock.getsockname())  # let TM know location
    sock.start_accepting()

    counter = 0  # number of executions made since last commit
    commit_time = 0  # keep track of time since last commit
    while True:
        # received KILL command from TaskManager
        if not status_queue.empty():
            status_queue.get()
            sock.close()
            drain_queue(sock.queue, curr, logger)
            break

        # no command for now -> sleep to avoid pegging CPU on blocking get
        if sock.queue.empty():
            time.sleep(0.001)

            # commit every five seconds to avoid blocking the db for too long
            if counter > 0 and time.time() - commit_time > 5:
                db.commit()
            continue

        # process query
        query = sock.queue.get()
        process_query(query, curr, logger)

        # batch commit if necessary
        counter += 1
        if counter >= commit_batch_size:
            counter = 0
            commit_time = time.time()
            db.commit()

    # finishes work and gracefully stops
    db.commit()
    db.close()


def process_query(query, curr, logger):
    """
    executes a query of form (template_string, arguments)
    query is of form (template_string, arguments)
    """
    if len(query) != 2:
        print("ERROR: Query is not the correct length")
        return
    statement = query[0]
    args = query[1]
    # This is the start of a browser
    if (statement == "start"):
        # crawl_id
        crawl_id = args
        print("crawl_id")
        print(crawl_id)
    elif (statement == "browserInfo"):
        filename = 'browserInfo.json'
        if (os.path.exists(filename)):
            f = open(filename, 'a')
            f.write(',')
        else:
            f = open(filename, 'w')
            f.write('[')
        f.write(args[0])
        f.close
    elif (statement == "FIN"):
        pass
    # When we get javascript data
    elif (query[1] == "crawl"):
        crawl_id = statement["crawl_id"]
        location = statement["location"]
        # If it is SQL command, drop it
        if (not location or not crawl_id):
            return
        # Hash URL so that it does not contain invalid char
        # print(location)
        name = hashlib.sha224(location).hexdigest()
        filename = "{}_{}.json".format(crawl_id, name)
        s3 = boto3.client('s3')
        # append_write = "w"
        # if (os.path.exists(filename)):
        #     append_write = 'a' # append if already exists
        # else:
		#     append_write = 'w' # make a new file if not
        # f = open(filename,append_write)
        # f.write(json.dumps(statement))
        # f.close()
        if (os.path.exists(filename)):
            f = open(filename, 'a')
            f.write(',')
        else:
            f = open(filename, 'w')
            f.write('[')
        f.write(json.dumps(statement))
        f.close
        # print(filename)
        for fn in os.listdir('.'):
           if os.path.isfile(fn):
            if (fn.startswith(str(crawl_id)) and fn != filename):
                f = open(fn, 'a')
                f.write(']')
                f.close()
                s3.upload_file(fn, "safe-ucosp-2017", fn)
                os.remove(fn)


    '''for i in range(len(args)):
        if isinstance(args[i], six.binary_type):
            args[i] = six.text_type(args[i], errors='ignore')
        elif callable(args[i]):
            args[i] = six.text_type(args[i])
    try:
        if len(args) == 0:
            curr.execute(statement)
        else:
            curr.execute(statement, args)

    except (OperationalError, ProgrammingError, IntegrityError) as e:
        logger.error(
            "Unsupported query:\n%s\n%s\n%s\n%s\n"
            % (type(e), e, statement, repr(args)))'''


def drain_queue(sock_queue, curr, logger):
    """ Ensures queue is empty before closing """
    time.sleep(3)  # TODO: the socket needs a better way of closing
    while not sock_queue.empty():
        query = sock_queue.get()
        process_query(query, curr, logger)
    print('Clean up')
    s3 = boto3.client('s3')
    for fn in os.listdir('.'):
        #print(fn)
        if (fn[-5:] == '.json'):
            f = open(fn, 'a')
            f.write(']')
            f.close()
            s3.upload_file(fn, "safe-ucosp-2017", fn)
            os.remove(fn)
	    # print("{} removed".format(fn))
