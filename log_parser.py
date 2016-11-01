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
        self.DATETIME_FORMAT = ''
        self.TIME_COL = 0
        self.TIME_SEPARATOR = ''
        self.DATE_SEPARATOR = ''
        self.COL_SEPARATOR = ''
        self.NUM_COL = 0
        self.COL_NAMES = []  # order matters!
        self.COL_TYPES = {}  # look up value type for each column
        self.timelog_lines = []
        self.LINE_NUM = 'line_num'

        with open(filename, 'r', 0) as f:
            self.timelog_lines = f.read().splitlines()

        self.log_df = pd.DataFrame({})

        self.LEGEND_LABELS = []
        self.fig = plt.figure()
        self.ax_list = [self.fig.add_subplot(111)]
        self.fig.suptitle(self.TIMELOG_FILENAME)

    def abstractTimeLogParser(self):
        # make placeholder lists, to be converted to dataframe later (there has to be a faster way!!)
        i = 0
        a_dict = {n: [] for n in self.COL_NAMES}
        a_dict[self.LINE_NUM] = []

        for line in self.timelog_lines:
            line = line.strip('\x00').strip()
            if line.count(self.COL_SEPARATOR) >= (len(self.COL_NAMES) - 1) \
                    and line.count(self.TIME_SEPARATOR) >= self.TIME_FORMAT.count(self.TIME_SEPARATOR):
                a_dict[self.LINE_NUM].append(i)
                for j in range(len(self.COL_NAMES)-1):
                    # -1 because last column does not need to be partitioned -
                    # this is to hedge against the column separator being used in the last column
                    keep, sep, line = line.partition(self.COL_SEPARATOR)
                    a_dict[self.COL_NAMES[j]].append(keep)
                a_dict[self.COL_NAMES[j+1]].append(line)

                i += 1

            # if line does not have a time value, does not have enough column separators, and is not the first line,
            # then it is wrapped from prev line
            elif i > 0:
                a_dict[self.COL_NAMES[-1]][i - 1] += line
            else:
                pass

        return pd.DataFrame(a_dict)

    def abstractTypeForce(self, a_df, columns=[], types={}):

        for c in columns:
            # time.clock shows that list comprehension method is slightly faster than pandas.apply method
            # time.clock also shows that converting to python timestamp takes the most time
            if types[c] == 0:
                ints = []
                for x in a_df[c]:
                    try:
                        ints.append(int(x))
                    except ValueError:
                        ints.append(float('nan'))
                a_df[c] = ints

            elif types[c] == 1:
                floats = []
                for x in a_df[c]:
                    try:
                        floats.append(float(x))
                    except ValueError:
                        floats.append(float('nan'))
                a_df[c] = floats

            elif types[c] == 2:
                a_df[c] = [str(x).upper() for x in a_df[c]]

            elif types[c] == 3:
                a_df[c] = [dt.datetime.strptime(x, self.TIME_FORMAT) for x in a_df[c]]

            elif types[c] == 4:
                a_df[c] = [dt.datetime.strptime(x, self.DATETIME_FORMAT) for x in a_df[c]]

            else:
                pass

        return a_df


    def abstractPlotHistory(self, time_vec, value_vec, color='None'):
        self.ax_list.append(plt.plot(time_vec, value_vec, color, mec='None'))

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
        self.TIME_SEPARATOR = ':'
        self.TIME_FORMAT = '%H:%M:%S'
        self.TIME_ZERO = dt.datetime.strptime('0:0:0', self.TIME_FORMAT)
        self.DATE_SEPARATOR = '/'
        self.DATE_FORMAT = '%m/%d/%Y'
        self.DATETIME_SEPARATOR = ' '
        self.DATETIME_FORMAT = self.DATE_FORMAT + self.DATETIME_SEPARATOR + self.TIME_FORMAT

        self.COL_SEPARATOR = '-'
        self.NUM_COL = 3
        self.COL_NAMES = ['session_num', 'time', 'msg']  # order matters!
        self.COL_TYPES = dict(zip(self.COL_NAMES, [0, 3, 2]))

        self.START_STR = ': begin()'
        self.COMPLETE_STR = ': complete called'
        self.SHUTDOWN_STR = ': shutdown requested'
        self.NCU_CLOCK = 'z,ct='
        self.XBEE_CLOCK = 'z,xt='
        self.XBEE_RESET = 'z,xt=0'
        self.INVALID_CLOCK = ':z,ct=2000/00/00'
        self.DEFAULT_PAN = ':z,pan=8888'
        self.DISCONNECTED = 'send: not connected'
        self.DISCOVERED_RESPONSE = ':9,'
        self.SCANNED = 'Scan received'
        self.FW_UPGRADE = 'update sesh data = d,'
        self.MONITOR_RESPONSE = '00000317'

        self.TASK_PUSHPARAMS = 'PushParamsHopefullyFaster'
        self.TASK_DISCOVER = 'DiscoverTaskNew'
        self.TASK_GET_NCU = 'GetNCUConfigTask'
        self.TASK_CONFIG_NCU = 'NCUConfigureTask'
        self.TASK_PAN_BC = 'pan broadcast'

        self.is_single_session = False
        self.is_valid_clock = True

        start_read = time.clock()
        self.parsed_df = self.abstractTimeLogParser()
        end_read = time.clock()
        print 'done reading file: ' + str(end_read - start_read)

        # force session numbers to ints and force capitalization on messages (makes is_in work better):
        start_force = time.clock()
        self.clean_df = self.abstractTypeForce(self.parsed_df,
                                                columns=['session_num', 'msg'],
                                                types=dict(zip(['session_num', 'msg'], [0, 2])))
        end_force = time.clock()
        print 'done forcing int or str type: ' + str(end_force - start_force)

        # Trying to figure out why this takes so long.
        # Maybe bc 1) doing operations with python datetime library, and/or
        # 2) accessing pandas df in a loop is expensive
        # seems that saving datetime conversion to the end saves the most time.
        # accessing lists instead of df doesn't make much difference
        start_dt = time.clock()

        # find_date_df: 1) does datetime conversion first,
        # 2) keeps df structure
        # self.find_date_df()
        #
        # find_datestr_df: 1) operates on times/dates as strings (leaves datetime conversion for later),
        # 2) keeps df structure
        self.find_datestr_df()
        #
        # find_date_list: 1) does datetime conversion first,
        # 2) converts from df to lists
        # self.find_date_list()
        #
        # find_datestr_list: 1) operates on times/dates as strings (leaves datetime conversion for later),
        # 2) converts from df to lists (must be aware that end index is excluded in lists, but included in df slicing!)
        # self.find_datestr_list()

        end_dt = time.clock()
        print 'done cleaning datetime: ' + str(end_dt - start_dt)

        self.plot_session_history()

    def find_date_df(self):
        new_datetime = []

        # 1) does datetime conversion first:
        timeclean_df = self.abstractTypeForce(self.clean_df,
                                               columns=['time'],
                                               types=dict(zip(['time'], [3])))
        # 2) keeps df structure
        a_df = timeclean_df

        # determine if app was restarted, or if log represents one contiguous session:
        new_session = a_df[a_df['session_num'] == 0]
        date_list = [dt.datetime.strptime(x.partition('STARTING... ')[2], self.DATE_FORMAT) for x in
                     new_session['msg']]  #### CHANGE FOR DATETIME/STR

        if len(new_session) > 0:  # multiple sessions (could be same day or many days)
            start_ix = new_session.index[0]
            # if first date instance is not found at beginning of file, backfill before first new_session:
            if start_ix > 0:
                start_ix = 0
                end_ix = new_session.index[0] - 1
                # is previous session likely same day (time t-1 <= time t) or previous day (time t-1 > time t)?
                if a_df.ix[end_ix, 'time'] > new_session.ix[end_ix + 1, 'time']:
                    date = date_list[0] - dt.timedelta(1)  #### CHANGE FOR DATETIME/STR
                else:
                    date = date_list[0]
                new_datetime.extend(
                    [date + (x - self.TIME_ZERO) for x in a_df.ix[start_ix:end_ix, 'time']])  #### CHANGE FOR DATETIME/STR AND DF/LIST

            for i in range(len(date_list)):
                start_ix = new_session.index[i]
                try:
                    end_ix = new_session.index[i + 1] - 1  # bc pandas df slicing is inclusive of endpoint
                except IndexError:
                    end_ix = a_df.index[-1]

                new_datetime.extend(
                    [date_list[i] + (x - self.TIME_ZERO) for x in a_df.ix[start_ix:end_ix, 'time']])  #### CHANGE FOR DATETIME/STR AND DF/LIST

        else:  # single session -- need to infer date from NCU clock
            self.is_single_session = True
            clock_times = self.find_keyword(self.NCU_CLOCK, 'msg')
            valid_clock = clock_times[[self.INVALID_CLOCK not in x for x in clock_times['msg']]]
            if len(valid_clock) > 0:
                self.is_valid_clock = True
                date_str = valid_clock.loc[valid_clock.index[0], 'msg'].partition(self.NCU_CLOCK.upper())[2].split('/')
                date = dt.datetime(int(date_str[0]), int(date_str[1]), int(date_str[2]))
                new_datetime = [date + (x - self.TIME_ZERO) for x in a_df['time']]   #### CHANGE FOR DATETIME/STR AND DF/LIST
            else:  # no valid clock available
                new_datetime = a_df['time']

        a_df.loc[:, 'datetime'] = new_datetime

        self.clean_df = a_df

    def find_date_list(self):
        new_datetime = []
        # 1) does datetime conversion first:
        timeclean_df = self.abstractTypeForce(self.clean_df,
                                                columns=['time'],
                                                types=dict(zip(['time'], [3])))
        a_df = timeclean_df
        # determine if app was restarted, or if log represents one contiguous session:
        new_session = a_df[a_df['session_num'] == 0]
        date_list = [dt.datetime.strptime(x.partition('STARTING... ')[2], self.DATE_FORMAT) for x in
                     new_session['msg']]  #### CHANGE FOR DATETIME/STR

        # 2) converts from df to lists (must be aware that end index is excluded in lists, but included in df slicing!)
        new_session_index_list = list(new_session.index)
        time_list = list(a_df['time'])
        index_list = list(a_df.index)

        if len(new_session) > 0:  # multiple sessions (could be same day or many days)
            start_ix = new_session_index_list[0]
            # if first date instance is not found at beginning of file, backfill before first new_session:
            if start_ix > 0:
                start_ix = 0
                end_ix = new_session_index_list[0] - 1  # bc list slicing is exclusive of endpoint
                # is previous session likely same day (time t-1 <= time t) or previous day (time t-1 > time t)?
                if a_df.ix[end_ix, 'time'] > new_session.ix[end_ix + 1, 'time']:
                    date = date_list[0] - dt.timedelta(1)  #### CHANGE FOR DATETIME/STR
                else:
                    date = date_list[0]
                new_datetime.extend(
                    [date + (x - self.TIME_ZERO) for x in
                     time_list[index_list.index(start_ix):index_list.index(end_ix)+1]])  #### CHANGE FOR DATETIME/STR AND DF/LIST

            for i in range(len(date_list)):
                start_ix = new_session_index_list[i]
                try:
                    end_ix = new_session_index_list[i + 1] - 1
                except IndexError:
                    end_ix = index_list[-1]

                new_datetime.extend(
                    [date_list[i] + (x - self.TIME_ZERO) for x in
                     time_list[index_list.index(start_ix):index_list.index(end_ix)+1]])  #### CHANGE FOR DATETIME/STR AND DF/LIST

        else:  # single session -- need to infer date from NCU clock
            self.is_single_session = True
            clock_times = self.find_keyword(self.NCU_CLOCK, 'msg')
            valid_clock = clock_times[[self.INVALID_CLOCK not in x for x in clock_times['msg']]]
            if len(valid_clock) > 0:
                self.is_valid_clock = True
                date_str = valid_clock.loc[valid_clock.index[0], 'msg'].partition(self.NCU_CLOCK.upper())[2].split('/')
                date = dt.datetime(int(date_str[0]), int(date_str[1]), int(date_str[2]))
                new_datetime = [date + (x - self.TIME_ZERO) for x in time_list]  #### CHANGE FOR DATETIME/STR AND DF/LIST
            else:  # no valid clock available
                new_datetime = a_df['time']

        a_df.loc[:, 'datetime'] = pd.Series(new_datetime, index=a_df.index)

        self.clean_df = a_df

    def find_datestr_df(self):
        # 1) operates on times/dates as strings (leaves datetime conversion for later),
        # 2) keeps df structure
        new_datetime = []

        # 2) keeps df structure
        a_df = self.clean_df

        # determine if app was restarted, or if log represents one contiguous session:
        new_session = a_df[a_df['session_num'] == 0]
        datestr_list = [x.partition('STARTING... ')[2] for x in new_session['msg']]  #### CHANGE FOR DATETIME OR DATESTR

        if len(new_session) > 0:  # multiple sessions (could be same day or many days)
            start_ix = new_session.index[0]
            # if first date instance is not found at beginning of file, backfill before first new_session:
            if start_ix > 0:
                start_ix = 0
                end_ix = new_session.index[0] - 1
                # is previous session likely same day (time t-1 <= time t) or previous day (time t-1 > time t)?
                if a_df.ix[end_ix, 'time'] > new_session.ix[end_ix + 1, 'time']:
                    datestr = (dt.datetime.strptime(datestr_list[0], self.DATE_FORMAT) - dt.timedelta(1)).strftime(self.DATE_FORMAT)  #### CHANGE FOR DATETIME OR DATESTR
                else:
                    datestr = datestr_list[0]
                new_datetime.extend(
                    [datestr + self.DATETIME_SEPARATOR + x
                     for x in a_df.ix[start_ix:end_ix, 'time']])  #### CHANGE FOR DATETIME OR DATESTR

            for i in range(len(datestr_list)):
                start_ix = new_session.index[i]
                try:
                    end_ix = new_session.index[i + 1] - 1  # bc pandas df slicing is inclusive of endpoint
                except IndexError:
                    end_ix = a_df.index[-1]

                new_datetime.extend(
                    [datestr_list[i] + self.DATETIME_SEPARATOR + x
                     for x in a_df.ix[start_ix:end_ix, 'time']])  #### CHANGE FOR DATETIME OR DATESTR

        else:  # single session -- need to infer date from NCU clock
            self.is_single_session = True
            clock_times = self.find_keyword(self.NCU_CLOCK, 'msg')
            valid_clock = clock_times[[self.INVALID_CLOCK not in x for x in clock_times['msg']]]
            if len(valid_clock) > 0:
                self.is_valid_clock = True
                # ncu clock msg is z,ct= yr/mo/day/hr/min/sec
                ncu_valid_clock = valid_clock.loc[valid_clock.index[0], 'msg'].partition(self.NCU_CLOCK.upper())[2].split(self.DATE_SEPARATOR)
                datestr = ncu_valid_clock[1] + self.DATE_SEPARATOR + ncu_valid_clock[0] + self.DATE_SEPARATOR + \
                          ncu_valid_clock[2]
                new_datetime = [datestr + self.DATETIME_SEPARATOR + x
                                for x in a_df.ix[:, 'time']]  #### CHANGE FOR DATETIME OR DATESTR
            else:  # no valid clock available
                new_datetime = a_df['time']

        a_df.loc[:, 'datetime'] = new_datetime

        # 2) datetime conversion, finally:
        if self.is_valid_clock is True:
            type_code = 4
        else:
            type_code = 3
        timeclean_df = self.abstractTypeForce(a_df,
                                              columns=['datetime'],
                                              types=dict(zip(['datetime'], [type_code])))
        self.clean_df = timeclean_df

    def find_datestr_list(self):

        new_datetime = []

        a_df = self.clean_df
        # determine if app was restarted, or if log represents one contiguous session:
        new_session = a_df[a_df['session_num'] == 0]
        datestr_list = [x.partition('STARTING... ')[2] for x in new_session['msg']]  #### CHANGE FOR DATETIME/STR

        # 2) converts from df to lists (must be aware that end index is excluded in lists, but included in df slicing!)
        new_session_index_list = list(new_session.index)
        time_list = list(a_df['time'])
        index_list = list(a_df.index)

        if len(new_session) > 0:  # multiple sessions (could be same day or many days)
            start_ix = new_session_index_list[0]
            # if first date instance is not found at beginning of file, backfill before first new_session:
            if start_ix > 0:
                start_ix = 0
                end_ix = new_session_index_list[0] - 1  # bc list slicing is exclusive of endpoint
                # is previous session likely same day (time t-1 <= time t) or previous day (time t-1 > time t)?
                if a_df.ix[end_ix, 'time'] > new_session.ix[end_ix + 1, 'time']:
                    datestr = (dt.datetime.strptime(datestr_list[0], self.DATE_FORMAT) - dt.timedelta(1)).strftime(
                        self.DATE_FORMAT)  #### CHANGE FOR DATETIME/STR
                else:
                    datestr = datestr_list[0]
                new_datetime.extend(
                    [datestr + self.DATETIME_SEPARATOR + x for x in
                     time_list[index_list.index(start_ix):index_list.index(
                         end_ix) + 1]])  #### CHANGE FOR DATETIME/STR AND DF/LIST

            for i in range(len(datestr_list)):
                start_ix = new_session_index_list[i]
                try:
                    end_ix = new_session_index_list[i + 1] - 1
                except IndexError:
                    end_ix = index_list[-1]

                new_datetime.extend(
                    [datestr_list[i] + self.DATETIME_SEPARATOR + x for x in
                     time_list[index_list.index(start_ix):index_list.index(
                         end_ix) + 1]])  #### CHANGE FOR DATETIME/STR AND DF/LIST

        else:  # single session -- need to infer date from NCU clock
            self.is_single_session = True
            clock_times = self.find_keyword(self.NCU_CLOCK, 'msg')
            valid_clock = clock_times[[self.INVALID_CLOCK not in x for x in clock_times['msg']]]
            if len(valid_clock) > 0:
                # ncu clock msg is z,ct= yr/mo/day/hr/min/sec
                ncu_valid_clock = valid_clock.loc[valid_clock.index[0], 'msg'].partition(self.NCU_CLOCK.upper())[
                    2].split(self.DATE_SEPARATOR)
                datestr = ncu_valid_clock[1] + self.DATE_SEPARATOR + ncu_valid_clock[0] + self.DATE_SEPARATOR + \
                          ncu_valid_clock[2]
                new_datetime = [datestr + self.DATETIME_SEPARATOR + x
                                for x in time_list]  #### CHANGE FOR DATETIME/STR AND DF/LIST
            else:  # no valid clock available
                self.is_valid_clock = False
                new_datetime = a_df['time']

        a_df.loc[:, 'datetime'] = pd.Series(new_datetime, index=a_df.index)

        # 2) datetime conversion, finally:
        if self.is_valid_clock is True:
            type_code = 4
        else:
            type_code = 3
        timeclean_df = self.abstractTypeForce(a_df,
                                              columns=['datetime'],
                                              types=dict(zip(['datetime'], [type_code])))

        self.clean_df = timeclean_df

    def plot_session_history(self):
        # plot each IP session independently
        ncu_conxn_df = self.get_ncu_connections()
        y_min = self.clean_df['session_num'].min()
        y_max = self.clean_df['session_num'].max()

        for n in range(len(ncu_conxn_df.index)):
            start = ncu_conxn_df.index[n]
            try:
                end = ncu_conxn_df.index[n + 1] - 1
            except IndexError:
                end = self.clean_df.index[-1]
            self.abstractPlotHistory(self.clean_df.loc[start:end, 'datetime'],
                                     self.clean_df.loc[start:end, 'session_num'], color='k-')

            text_label_x = ncu_conxn_df.loc[start, 'datetime']
            text_label_y = ncu_conxn_df.loc[start, 'session_num']
            plt.text(text_label_x, text_label_y + (y_max - y_min)/100, ncu_conxn_df.loc[start, 'ip'],
                              fontsize=8, rotation='vertical',
                              horizontalalignment='center',
                              verticalalignment='bottom')  # vertical offset for visual clarity
            plt.plot(text_label_x, text_label_y,
                              marker='d', mec='k', color='None')

    def get_ncu_connections(self):
        # get ncu connections in order that they occurred, and associated datetimes
        # todo: need to handle multiple TCX windows open connected to multiple NCUs at once
        # if new connection and < x seconds since last conxn, likely bc multiple windows w/ diff NCUs are open
        # todo: what if multiple windows w/ same ncu?  that's ok, just looks ugly on plot
        start = time.clock()
        all_ncus = self.find_keyword('v,0', 'msg')
        ncu_list = [x.partition('RECEIVED FROM ')[2].partition(':V,0')[0] for x in all_ncus.loc[:, 'msg']]
        all_ncus.loc[:, 'ip'] = pd.Series(ncu_list, index=all_ncus.index)
        end = time.clock()
        print 'find all ncu: ' + str(end - start)

        # check if message is to/from a different ip address than the previous message:
        start = time.clock()
        is_new_connection = [True]*len(all_ncus)
        for n in range(1, len(ncu_list)):
            if ncu_list[n] != ncu_list[n-1]:
                is_new_connection[n] = True
            else:
                is_new_connection[n] = False
        conxn_df = all_ncus[is_new_connection]
        end = time.clock()
        print 'new conxn: ' + str(end - start)

        # sort to group by ip addresses
        sorted_conxn_df = conxn_df.sort_values(['ip', 'line_num'])

        # check if ip address is different, or that time betwn messages of from same ip address is long enough
        # to merit a new conxn:
        # TODO: OPTIMIZE!! THIS TAKES TOO LONG.  slow down is in iterating over the OR conditional
        threshold = dt.timedelta(seconds=4)
        is_distinct_conxn = [True]*len(sorted_conxn_df)
        ip_list = list(sorted_conxn_df['ip'])
        datetime_list = list(sorted_conxn_df['datetime'])

        for s in range(1, len(sorted_conxn_df)):
            # accessing df: it seems that there is some overhead that causes this to be slow!
            # is_distinct_conxn[s] = (sorted_conxn_df.iloc[s, :]['ip'] != sorted_conxn_df.iloc[s - 1, :]['ip']) or \
            #                        (sorted_conxn_df.iloc[s, :]['datetime'] - sorted_conxn_df.iloc[s - 1, :]['datetime'] > threshold)
            # accessing list:
            is_distinct_conxn[s] = (ip_list[s] != ip_list[s-1]) or (datetime_list[s] - datetime_list[s - 1]) > threshold

        return sorted_conxn_df[is_distinct_conxn].sort_values('line_num')

    def plot_ncu_connection(self):
        ncu_connections_df = self.get_ncu_connections()

        for n in ncu_connections_df.index:
            text_label_x = ncu_connections_df.loc[n, 'datetime']
            text_label_y = ncu_connections_df.loc[n, 'session_num']
            self.ax_list.text(text_label_x, text_label_y + 1000, ncu_connections_df.loc[n, 'ip'],
                              fontsize=8, rotation='vertical',
                              horizontalalignment='center',
                              verticalalignment='bottom')  # vertical offset for visual clarity
            self.ax_list.plot(text_label_x, text_label_y,
                              marker='d', mec='k', color='None')
        self.LEGEND_LABELS += ['ip']
        # handles, labels = self.ax.get_legend_handles_labels()
        # print handles, labels
        # self.ax.legend(handles, labels)

    def get_ncu_list(self):
        ncu_df = self.get_ncu_connections()
        return list(set(ncu_df['ip']))

    def split_by_ncu(self):
        ncu_list = self.get_ncu_list()
        return {n: self.find_keyword(n, 'msg') for n in ncu_list}

    def find_keyword(self, keyword, column_name):
        return self.clean_df[[keyword.upper() in x for x in self.clean_df[column_name]]]

    def plot_keyword_history(self, keyword, marker_format):
        keyword_df = self.find_keyword(keyword, 'msg')
        self.abstractPlotHistory(keyword_df['datetime'], keyword_df['session_num'], marker_format)
        # handles, labels = self.ax.get_legend_handles_labels()
        # print handles, labels
        self.LEGEND_LABELS += [keyword]
        plt.legend(self.LEGEND_LABELS, loc='best')

    def get_spc_list(self):
        all_spcs_msg = self.find_keyword('SPC', 'msg').loc[:, 'msg']
        spc_list = ['SPC' + x.partition('SPC')[2][:13] for x in all_spcs_msg]

        # todo: watch out for messages that list the number of SPCs loaded
        return list(set(spc_list))

    def get_bc_commands(self):
        # get unique list of broadcast commands sent during session
        all_bc = self.find_keyword('0000FFFF,', 'msg').loc[:, 'msg']
        bc_list = [x.partition('0000FFFF,')[2] for x in all_bc]

        # todo: figure out which command means what

        return list(set(bc_list))

    def collate_messages(self):
        # find all received msgs that have the format: command, destination, source, information
        # include time stamp
        # package as dataframe
        # can then slice dataframe in interesting ways, i.e. unique destinations, all inf from one destination, etc.
        pass

    def get_discovery_rate(self):
        # for each ncu_session, find start and end of discovery session,
        # count number discovered,
        # return average rate
        pass



# filename = 'TrackerCx_jc_2016-10-17.log'
# test = TCX_TimeLogReader(filename)
