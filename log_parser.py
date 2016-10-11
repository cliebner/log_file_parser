__author__ = 'christina'


"""
Started: 5 Oct 2016

Define the most abstract log file with time-dependent data.

Applications:
For Tcx, we can parse the log file to gather data on how users are using TCx and what they are experiencing.
Interesting stats:
* number of NCUs/day
* time to discover
* time to do any other task
* order of operations
* number of crashes

Assume:
* Any file has a set of message types, where the number of message types is less than or equal to the number of messages (lines) in the file.
* The user must define message types.  Any line that does not meet a message type definition is ignored.
* An action (e.g. make a graph) can be defined for any class of message types.

Main loop:
1. Read line
2. copy lines into buckets (or keep one copy, tag it with potential types, then re-read lines to grab tags you care about?)
3. keep track of date & time (watch out for lines that don't have a time stamp! assign to previous line's time!)
4.
5. run visualizations and any other calculations according to manufactured types

possible message types:
* tcx action that has a start and an end
** Discovery,
** PushParamsHopefullyFaster: PushParamsHopefullyFaster: begin(), PushParamsHopefullyFaster: Run()
* message pertaining to a specific (user defined by user input?) spc
* broadcast message
* sent messages
* received messages
* same IP address
* within a single session
* on the same day
* AT commands

(custom datetimeFixer) Enforce dates:
1. scan file, look for 00000 and grab dates. make look up table for line number and date
2. between line numbers, apply date
3. before first 0000, compare timestamp. if sequential, there's a good chance it's the same day. otherwise, assign previous day
4. from last 00000 and end of file, apply last date
5. print/save range of datetimes covered by file

every line has:
1. line number in file
2. message

In tcx logs, the message is further parsed as:
a. session record number
b. time stamp
c. (calculated date stamp)
d. send/receive message

"""
import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd


class abstractTimeLogReader(object):
    def __init__(self, filename):
        self.TIMELOG_FILENAME = filename
        self.timelog_lines = []

        with open(filename, 'r', 0) as f:
            self.timelog_lines = f.read().splitlines()
        # f = open(self.TIMELOG_FILENAME, 'r', 0)
        #
        # # remove blank lines
        # for line in f:
        #     if len(line.strip()) > 0:
        #         self.timelog_lines.append(line.strip())
        # f.close()


class TCX_TimeLogReader(abstractTimeLogReader):
    # TCX_specific methods, or TCX-specific tweaks to methods in abstract
    def __init__(self, filename):
        super(TCX_TimeLogReader, self).__init__(filename)
        self.TIME_FORMAT = '%H:%M:%S'
        self.DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
        self.clean_df = self.clean_TCX_lines()

    def clean_TCX_lines(self):

        # make placeholder lists, to be converted to dataframe later (there has to be a faster way!!)
        i = 0
        line_num = []
        session_num = []
        time = []
        msg = []
        for line in self.timelog_lines:
            if line.count('-') >= 2:
                line_num.append(i)
                session_num.append(line.partition('-')[0])
                time.append(line.partition('-')[2].partition('-')[0])
                msg.append(line.partition('-')[2].partition('-')[2])
                i += 1
            elif len(line.split('-')) == 1 and i > 0:  # line is not empty, not the first line, and wrapped from prev line
                msg[i-1] += line
            else:
                pass

        # make pandas dataframe with columns: line #, log session #, time, msg
        return pd.DataFrame({'line_num': pd.Series(line_num),
                             'session_num': pd.Series(session_num),
                             'time': pd.Series(time),
                             'msg': pd.Series(msg)})



def abstractDateTimeFixer(object):
    # apply user-defined date time fixer method to the lines in the read-in file
    pass

class abstractBucket(object):
    # do in a loop based on user input?
    # newBucketClass = bucketClassFactory()
    pass

def bucketFactory(object):
    # make buckets based on user's rules! in a json perhaps??
    pass

filename = 'TrackerCx_brian_2016-10-03.log'
briantest = TCX_TimeLogReader(filename)