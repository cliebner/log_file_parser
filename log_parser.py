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
        self.TIME_FORMAT = ''
        self.TIME_COL = 0
        self.SEPARATOR = ''
        self.NUM_COL = 0
        self.COL_NAMES = []  # order matters!
        self.COL_TYPES = {}  # look up value type for each column
        self.timelog_lines = []

        with open(filename, 'r', 0) as f:
            self.timelog_lines = f.read().splitlines()

    def abstractTimeLogCleaner(self):
        # make placeholder lists, to be converted to dataframe later (there has to be a faster way!!)
        i = 0
        a_dict = {n: [] for n in self.COL_NAMES}
        a_dict['line_num'] = []

        for line in self.timelog_lines:
            if line.count(self.SEPARATOR) >= (self.NUM_COL - 1):
                a_dict['line_num'].append(i)
                for j in range(len(self.COL_NAMES)-1):  # last column does not need to be partitioned - this is to hedge against separator being used in the last column
                    keep, sep, line = line.partition(self.SEPARATOR)
                    a_dict[self.COL_NAMES[j]].append(keep)
                a_dict[self.COL_NAMES[j+1]].append(line)

                i += 1

            # line is not empty, not the first line, and wrapped from prev line
            elif len(line.split(self.SEPARATOR)) == 1 and i > 0:
                a_dict[self.COL_NAMES[-1]][i - 1] += line  # TODO: make sure types are the same! (should be str)
            else:
                pass

        for c in self.COL_NAMES:
            # TODO: implement MORE ELEGANT type forcing from column_type_dict
            if self.COL_TYPES[c] == 0:
                a_dict[c] = [int(x) for x in a_dict[c]]
            elif self.COL_TYPES[c] == 1:
                a_dict[c] = [float(x) for x in a_dict[c]]
            elif self.COL_TYPES[c] == 2:
                a_dict[c] = [str(x).upper() for x in a_dict[c]]
            elif self.COL_TYPES[c] == 3:
                a_dict[c] = [dt.datetime.strptime(x, self.TIME_FORMAT) for x in a_dict[c]]
            else:
                pass
        return pd.DataFrame(a_dict)



    def abstractPlotHistory(self, time_vec, value_vec, color='None'):
        plt.plot(time_vec, value_vec, color, mec='None')
        plt.title(self.TIMELOG_FILENAME)

    def abstractDateTimeFixer(self):
        # apply user-defined date time fixer method to the lines in the read-in file
        pass

    def abstractBucket(self):
        # do in a loop based on user input?
        # newBucketClass = bucketClassFactory()
        pass

    def bucketFactory(self):
        # make buckets based on user's rules! in a json perhaps??
        pass


class TCX_TimeLogReader(abstractTimeLogReader):
    # TCX_specific methods, or TCX-specific tweaks to methods in abstract
    def __init__(self, filename):
        super(TCX_TimeLogReader, self).__init__(filename)
        self.filename = filename
        self.TIME_FORMAT = '%H:%M:%S'
        self.TIME_ZERO = dt.datetime.strptime('0:0:0', self.TIME_FORMAT)
        self.DATE_FORMAT = '%m/%d/%Y'
        self.DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

        self.SEPARATOR = '-'
        self.NUM_COL = 3
        self.COL_NAMES = ['session_num', 'time', 'msg']  # order matters!
        self.COL_TYPES = dict(zip(self.COL_NAMES, [0, 3, 2]))

        self.START_STR = ': begin()'.upper()
        self.COMPLETE_STR = ': complete called'.upper()

        self.TASK_PUSHPARAMS = 'PushParamsHopefullyFaster'.upper()
        self.TASK_DISCOVER = 'DiscoverTaskNew'.upper()

        self.clean_df = self.abstractTimeLogCleaner()

    def plot_session_history(self):
        self.abstractPlotHistory(self.clean_df['time'], self.clean_df['session_num'], color='k')

    def plot_keyword_history(self, keyword, marker_format):
        keyword_df = self.find_keyword(keyword, 'msg')
        self.abstractPlotHistory(keyword_df['time'], keyword_df['session_num'], marker_format)
        plt.title(self.filename)

    def find_keyword(self, keyword, column_name):
        return self.clean_df[[keyword.upper() in x for x in self.clean_df[column_name]]]

    def get_spc_list(self):
        all_spcs_msg = self.find_keyword('SPC', 'msg').loc[:, 'msg']
        spc_list = ['SPC'+x.partition('SPC')[2][:13] for x in all_spcs_msg]

        # todo: watch out for messages that list the number of SPCs loaded
        return list(set(spc_list))

    def get_ncu_list(self):
        # get unique list of ip addresses connected to during session
        all_ncus = self.find_keyword('v,0', 'msg').loc[:, 'msg']
        ncu_list = [x.partition('RECEIVED FROM ')[2].partition(':V,0')[0] for x in all_ncus]
        return list(set(ncu_list))

    def get_bc_commands(self):
        # get unique list of broadcast commands sent during session
        all_bc = self.find_keyword('0000FFFF,', 'msg').loc[:, 'msg']
        bc_list = [x.partition('0000FFFF,')[2] for x in all_bc]

        #todo: figure out which command means what

        return list(set(bc_list))

    def fix_date(self):
        new_start = self.clean_df[self.clean_df['session_num'] == 0]
        date_list = [dt.datetime.strptime(x.partition('STARTING... ')[2], '%m/%d/%Y') for x in new_start['msg']]
        # before the first start instance:
        end = new_start.index[0]-1

        # between each start:
        # after the last start:

    def collate_messages(self):
        # find all received msgs that have the format: command, destination, source, information
        # include time stamp
        # package as dataframe
        # can then slice dataframe in interesting ways, i.e. unique destinations, all inf from one destination, etc.
        pass




filename = 'TrackerCx_brian_2016-10-03.log'
briantest = TCX_TimeLogReader(filename)
brian_spcs = briantest.get_ncu_list()