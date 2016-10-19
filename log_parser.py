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
import time


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
        self.LINE_NUM = 'line_num'

        # start = time.clock()

        with open(filename, 'r', 0) as f:
            self.timelog_lines = f.read().splitlines()
        # think about pickling, save as cpickle
        # end = time.clock()
        # print (end - start)

    def abstractTimeLogCleaner(self):
        # make placeholder lists, to be converted to dataframe later (there has to be a faster way!!)
        i = 0
        a_dict = {n: [] for n in self.COL_NAMES}
        a_dict[self.LINE_NUM] = []

        start = time.clock()
        for line in self.timelog_lines:
            line = line.strip('\x00').strip()
            if line.count(self.SEPARATOR) >= (self.NUM_COL - 1):
                a_dict[self.LINE_NUM].append(i)
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
        end = time.clock()
        print 'clean + sort: ' + str(end - start)

        for c in self.COL_NAMES:
            # TODO: implement MORE ELEGANT type forcing from column_type_dict
            # time.clock shows that list comprehension method is slightly faster than pandas.apply method
            # time.clock also shows that converting to python timestamp takes the most time
            if self.COL_TYPES[c] == 0:
                # start = time.clock()
                a_dict[c] = [int(x) for x in a_dict[c]]
                # end = time.clock()
                # print ('int: ' + str(end - start))

            elif self.COL_TYPES[c] == 1:
                # start = time.clock()
                a_dict[c] = [float(x) for x in a_dict[c]]
                # end = time.clock()
                # print ('float: ' + str(end - start))

            elif self.COL_TYPES[c] == 2:
                # start = time.clock()
                a_dict[c] = [str(x).upper() for x in a_dict[c]]
                # end = time.clock()
                # print ('str: ' + str(end - start))

            elif self.COL_TYPES[c] == 3:
                # start = time.clock()
                a_dict[c] = [dt.datetime.strptime(x, self.TIME_FORMAT) for x in a_dict[c]]
                # end = time.clock()
                # print ('timestamp: ' + str(end - start))

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

        self.START_STR = ': begin()'
        self.COMPLETE_STR = ': complete called'
        self.SHUTDOWN_STR = ': shutdown requested'
        self.CLOCK = 'z,ct='
        self.INVALID_CLOCK = ':z,ct=2000/00/00'
        self.DEFAULT_PAN = ':z,pan=8888'
        self.DISCONNECTED = 'send: not connected'
        self.DISCOVERED_RESPONSE = ':9,'
        self.SCANNED = 'Scan received'
        self.FW_UPGRADE = ':d,'

        self.TASK_PUSHPARAMS = 'PushParamsHopefullyFaster'
        self.TASK_DISCOVER = 'DiscoverTaskNew'
        self.TASK_GET_NCU = 'GetNCUConfigTask'
        self.TASK_CONFIG_NCU = 'NCUConfigureTask'

        self.is_single_session = False
        self.is_valid_clock = False

        self.clean_df = self.abstractTimeLogCleaner()
        self.fix_datetime()

        print('done')

    def plot_session_history(self):
        self.abstractPlotHistory(self.clean_df['datetime'], self.clean_df['session_num'], color='k')
        self.plot_ncu_connection()

    def plot_keyword_history(self, keyword, marker_format):
        keyword_df = self.find_keyword(keyword, 'msg')
        self.abstractPlotHistory(keyword_df['datetime'], keyword_df['session_num'], marker_format)
        plt.title(self.filename)

    def find_keyword(self, keyword, column_name):
        return self.clean_df[[keyword.upper() in x for x in self.clean_df[column_name]]]

    def get_spc_list(self):
        all_spcs_msg = self.find_keyword('SPC', 'msg').loc[:, 'msg']
        spc_list = ['SPC'+x.partition('SPC')[2][:13] for x in all_spcs_msg]

        # todo: watch out for messages that list the number of SPCs loaded
        return list(set(spc_list))

    def get_ncu_connections(self):
        # get ncu connections in order that they occurred, and associated datetimes

        all_ncus = self.find_keyword('v,0', 'msg')
        ncu_list = [x.partition('RECEIVED FROM ')[2].partition(':V,0')[0] for x in all_ncus.loc[:, 'msg']]
        all_ncus['ip'] = ncu_list

        is_switch = [True]
        for n in range(1, len(ncu_list)):
            if ncu_list[n] != ncu_list[n-1]:
                is_switch.append(True)
            else:
                is_switch.append(False)

        return all_ncus[is_switch]

    def get_ncu_list(self):
        ncu_df = self.get_ncu_connections()
        return list(set(ncu_df['ip']))

    def plot_ncu_connection(self):
        ncu_connections_df = self.get_ncu_connections()
        cols = ncu_connections_df.columns
        time_index = cols.index()
        for n in ncu_connections_df.itertuples():

            # todo: what if user goes back to an ncu? need to catch changes in ip address, not just first instance
            text_label_x = ncu_session.loc[ncu_session.index[0], 'datetime']
            text_label_y = ncu_session.loc[ncu_session.index[0], 'session_num']
            plt.text(text_label_x, text_label_y + 1000, n,
                     fontsize=8, rotation='vertical',
                     horizontalalignment='center', verticalalignment='bottom')  # vertical offset for visual clarity
            plt.plot(text_label_x, text_label_y,
                     marker='d', mec='k', color='None')


    def get_bc_commands(self):
        # get unique list of broadcast commands sent during session
        all_bc = self.find_keyword('0000FFFF,', 'msg').loc[:, 'msg']
        bc_list = [x.partition('0000FFFF,')[2] for x in all_bc]

        # todo: figure out which command means what

        return list(set(bc_list))

    def fix_datetime(self):
        new_datetime = []
        # determine if app was restarted, or if log represents one contiguous session:
        new_session = self.clean_df[self.clean_df['session_num'] == 0]

        if len(new_session) > 0:  # multiple sessions (could be same day or many days)
            date_list = [dt.datetime.strptime(x.partition('STARTING... ')[2], '%m/%d/%Y') for x in new_session['msg']]

            for i in range(len(date_list) + 1):
                if i == 0:  # start
                    start_ix = 0
                    end_ix = new_session.index[0] - 1

                    # is previous session likely same day (time t-1 <= time t) or previous day (time t-1 > time t)?
                    if self.clean_df.ix[end_ix, 'time'] > new_session.ix[end_ix+1, 'time']:
                        date = date_list[0] - dt.timedelta(1)
                    else:
                        date = date_list[0]
                elif i == len(date_list):  # end
                    start_ix = new_session.index[i - 1]
                    end_ix = self.clean_df.index[-1]
                    date = date_list[i - 1]
                else:  # middle
                    start_ix = new_session.index[i - 1]
                    end_ix = new_session.index[i] - 1
                    date = date_list[i - 1]

                new_datetime.extend(
                    [date + (x - self.TIME_ZERO) for x in self.clean_df.ix[start_ix:end_ix, 'time']])

        else:  # single session -- need to infer date from NCU clock
            self.is_single_session = True
            clock_times = self.find_keyword(self.CLOCK, 'msg')
            valid_clock = clock_times[[self.INVALID_CLOCK not in x for x in clock_times['msg']]]
            if len(valid_clock) > 0:
                self.is_valid_clock = True
                date_str = valid_clock.loc[valid_clock.index[0], 'msg'].partition(self.CLOCK.upper())[2].split('/')
                date = dt.datetime(int(date_str[0]), int(date_str[1]), int(date_str[2]))
                new_datetime = [date + (x - self.TIME_ZERO) for x in self.clean_df['time']]
            else:  # no valid clock available
                new_datetime = self.clean_df['time']

        self.clean_df['datetime'] = new_datetime

    def collate_messages(self):
        # find all received msgs that have the format: command, destination, source, information
        # include time stamp
        # package as dataframe
        # can then slice dataframe in interesting ways, i.e. unique destinations, all inf from one destination, etc.
        pass

    def plot_discovery(self):
        # plot discovered time vs spc name for all SPCs (in session? per NCU?)
        # interesting to see how discovery events cluster

        # What "received" message do we get during DiscoveryTaskNew?
        pass




# filename = 'TrackerCx_jc_2016-10-17.log'
# test = TCX_TimeLogReader(filename)
# test.plot_session_history()
# test.plot_ncu_list()
