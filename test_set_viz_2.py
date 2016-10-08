__author__ = 'christina'


"""
This class defines a set of text files as an object that describes a test set.
Methods in this class read the text files and allow the user to create visual representations of what the scheduler did,
when prerequisites become in/valid, and to print basic statistics about the test set.

Features:
Read in Test Set Log file. (minimum requirement)
Read in n Prereq Validity text file(s).
Read in n Test Log text file(s).

Get basic test set stats: start/end time, avg test run time
Get graph of tests and prereqs on timeline (optionally: with prereq validity data superimposed)
Get graph of num tests run simultaneously (count of running =[])

Must be adaptable in both 1.0 and 1.1.

Some methods require certain type of text files be read in. If the required file is not read in, then the method should
return None or [].

User has to define the text file type so that the class can tell which methods apply.

"""
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np

RESULT = 'result'
TIME = 'time'
VALUE = 'value'
RESULT_TIME = "result_time"
RESULT_VALUE = "result_value"
VALID_TIME = 'valid_time'
VALID_VALUE = 'valid_value'
MAX_SIMUL_TESTS = 26
COLORS_ANY = ['b', 'g', 'r', 'c', 'm', 'y']
COLORS_HOT = ['#ff0000', '#ff6600', '#990000', '#ff3300', '#800000', '#cc3300']
COLORS_COOL = ['#0000ff', '#3366ff', '#000099', '#0099ff']
COLORS_AIR = ['#006600', '#00cc00', '#003300', '#009900']
color_dict = {
    'any': COLORS_ANY,
    'hot': COLORS_HOT,
    'cool': COLORS_COOL,
    'air': COLORS_AIR
}
AIR_FLAG = 'DuctPressure'
HOT_FLAG = 'Hot'
COOL_FLAG = 'ColdDuctTemperature'
TEST_TYPE = {
    'any': ['CST', 'DAT', 'ZAT', 'CO2', 'OCC', 'RH'],
    'air': ['AFC', 'DPC'],
    'cool': ['ZSA'],
    'hot': ['AFH', 'DPH', 'HWV', 'ELS-1', 'ELS-2', 'ELS-3', 'EL-M']
}

Y_TICK_LO = 0.0
Y_TICK_HI = 1.0

class TestSet(object):
    def __init__(self, filename, version):
        self.state_machine_dict = {
            'v1.0': "2set CXTest state machine running ",
            'v1.1': "2SCXTest running "
        }
        self.modeled_eq_dict = {
            'v1.0': "<ModeledEquipment: ",
            'v1.1': "<Equipment: "
        }
        self.LOCKED = 'Found locked zone: '
        self.MODELED_EQ = self.modeled_eq_dict[version]
        self.PREREQ_ID = "updating prereq "
        self.PREREQ_MACH = "<PrereqMachine: "  # used to parse prereq log entry into each prereq provider
        self.PREREQ_MACH_LIST = "{<PrereqMachine: "  # used to identify log entries that print the prereq sets
        self.RUNNING = "running = "
        self.LOG_LINE_SEPARATOR = ' - '
        self.STATE_MACHINE = self.state_machine_dict[version]
        self.TEST_MESSAGE = ["Test analysis complete.", "Setting final result to : "]
        self.TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
        self.TO_RUN = 'to run = '
        self.PREREQ_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
        self.RESULT_FORMAT = {
            "Result: passed": {
                'color': '#00e402',
                'marker': 'o'
            },
            "Result: failed": {
                'color': '#e51e05',
                'marker': 's'
            },
            "2": {
                'color': '#ffcf12',
                'marker': '^'
            }
        }
        self.TEST_LOG_FILENAME = filename  # TODO: throw error if file is not plaintext type
        self.TO_RUN = "to run = "
        self.UNLOCKED = "Found unlocked zone: "  # note final whitespace
        self.VERSION = version

        # initialize safety set dict, test set dict, test counters, index counters, etc.
        self.unlocked_zone_list = []
        self.locked_zone_list = []
        self.is_to_run = False
        self.test_count_dict = {}
        self.test_set_dict = {}
        self.safety_set_dict = {}
        self.prereq_validity_data = {}
        self.test_result_dict = {}

        with open(self.TEST_LOG_FILENAME,'r',0) as f:
            self.test_log_list = f.read().splitlines()

        # using built-in method partition instead of split lets us ignore any occurences of the log line separator that exist in the line message
        (start, sep, rest) = self.test_log_list[0].partition(self.LOG_LINE_SEPARATOR)[-1].partition(self.LOG_LINE_SEPARATOR)
        self.TEST_START = self.convert_datetime(start)
        (end, sep, rest) = self.test_log_list[-2].partition(self.LOG_LINE_SEPARATOR)[-1].partition(self.LOG_LINE_SEPARATOR)
        self.TEST_END = self.convert_datetime(end)

        # for each line in log : parse line with if/elif statements
        for line in self.test_log_list:

            if len(line) > 0:
                (line_time, sep, line_message) = line.partition(self.LOG_LINE_SEPARATOR)[-1].partition(self.LOG_LINE_SEPARATOR)
                line_datetime = self.convert_datetime(line_time)

            # Parse Locked Zone Avoider to create equipment_to_run list
            # (at end, if ignore_locked == True, then mush together unlocked and locked list for final equipment_to_run
            if self.UNLOCKED in line_message:
                self.read_unlocked_zones(line_message)
            elif self.LOCKED in line_message:
                self.read_locked_zones(line_message)

            # Parse: tests to run
            elif self.TO_RUN in line_message and not self.is_to_run:
                self.read_scheduled(line_message, line_datetime)  # how to make user not call this method? make it only used inside class instantiation?

            # Parse: running (at end, compare to to_run to see what didn't end up running)
            elif self.RUNNING in line_message:
                self.read_test_set(line_message, line_datetime)  # how to make user not call this method? make it only used inside class instantiation?

            # Parse: Prereq machine
            elif self.PREREQ_MACH_LIST in line_message:
                self.read_safety_set(line_message, line_datetime)  # how to make user not call this method? make it only used inside class instantiation?

            else:
                pass


    def __str__(self):
        return self.VERSION + " Test Set: " + self.TEST_LOG_FILENAME.rstrip('.txt')

    def read_locked_zones(self, line_message):
        if line_message.lstrip(self.LOCKED) not in self.locked_zone_list:
            self.locked_zone_list.append(line_message.lstrip(self.LOCKED))

    def read_unlocked_zones(self, line_message):
        if line_message.lstrip(self.UNLOCKED) not in self.unlocked_zone_list:
            self.unlocked_zone_list.append(line_message.lstrip(self.UNLOCKED))

    def read_safety_set(self, line_message, line_datetime, validity_data=False):
        """
        For each log entry with prereq data:
        1. parse line into prereq machine segments
        2. parse prereq machine segments into equipment ref names
        3. append the datetime value to each list

        safety_set_dict = {
            box ref name = {
                prereq ID name = {
                    TIME: list of datetime type values when state machine had this box as safety set for prereq ID
                    VALUE: will hold the y
                }
            }
        }
        """
        all_safety_sets = line_message.lstrip('{').rstrip('}').replace(', ', '').split(self.PREREQ_MACH)[1:]
        for a_safety_set in all_safety_sets:
            (prereq_ID, safety_box_list) = self.parse_prereqs(a_safety_set)
            for box in safety_box_list:
                try:
                    self.safety_set_dict[box]
                except KeyError:  # need to initialize a dict for the new box
                    self.safety_set_dict[box] = {}
                try:
                    self.safety_set_dict[box][prereq_ID]
                except KeyError:  # need to initialize a list for the new test
                    self.safety_set_dict[box][prereq_ID] = {
                        VALUE : None,
                        TIME : []  # will put lean datetime list here
                    }
                self.safety_set_dict[box][prereq_ID][TIME].append(line_datetime)
                # TODO: set [VALUE] when boxes are ordered alphanumerically. for plotting y-axis

    def read_scheduled(self, line_message, line_datetime):
        # remove square brackets and parse line into test segments
        test_instances_list = line_message.lstrip('[').rstrip(']').replace(', ', '').split(self.STATE_MACHINE)[1:]
        for instance in test_instances_list:
            (test, sep, box) = instance.partition(' on ')
            # initialize test_set dict and test_count dict
            try:
                self.test_set_dict[box]
            except KeyError:  # need to initialize a dict for the new box
                self.test_set_dict[box] = {}
            try:
                self.test_set_dict[box][test]
            except KeyError:  # need to initialize a list for the new test
                self.test_set_dict[box][test] = {
                    VALUE : None,
                    TIME : []  # will put lean datetime list here
                }
            try:
                self.test_count_dict[test]
            except KeyError:
                self.test_count_dict[test] = {
                    VALUE : [0],
                    TIME : [line_datetime]
            }
        self.test_count_dict['all'] = {
            VALUE : [0],
            TIME : [line_datetime]
        }

        self.is_to_run = True

    def read_test_set(self, line_message, line_datetime):
        # elif self.RUNNING in line_message and line_message not in unique_running_messages:
        # unique_running_messages.append(line_message)
        # remove square brackets and parse line into test segments
        test_instances_list = line_message.lstrip('[').rstrip(']').replace(', ', '').split(self.STATE_MACHINE)[1:]

        for test in self.test_count_dict.keys():
            if test is 'all':
                self.test_count_dict[test][VALUE].append(len(test_instances_list))
            else:
                self.test_count_dict[test][VALUE].append(line_message.count(test)),
            self.test_count_dict[test][TIME].append(line_datetime)

        for instance in test_instances_list:
            (test, sep, box) = instance.partition(' on ')
            self.test_set_dict[box][test][TIME].append(line_datetime)
            # TODO: set [VALUE] when boxes are ordered alphanumerically. for plotting y-axis

    def parse_prereqs(self, seg):
        (prereq_ID, sep, equip) = seg.partition('>: ')
        equipments = equip.lstrip('[').rstrip(']')
        if len(equipments) == 0:
            refnames_list = ['Manual']
        else:
            refnames_list = equipments.replace('>', '').split(self.MODELED_EQ)
            refnames_list.remove('')
        return prereq_ID, refnames_list

    def get_prereq_IDs(self):
        """
        finds all the unique prereq ids in the test set and returns them as a set
        """
        unique_prereqs = {x for y in self.safety_set_dict.keys()
                          for x in self.safety_set_dict[y].keys()}
        return unique_prereqs

    def get_safety_set_box_list(self):
        return self.safety_set_dict.keys()

    def get_scheduled_test_list(self):
        return self.test_count_dict.keys()

    def get_scheduled_box_list(self):
        return self.test_set_dict.keys()

    def get_sorted_box_list(self):
        return sorted(list(set(self.safety_set_dict.keys() + self.test_set_dict.keys())))

    def convert_datetime(self, aStr):
        # clip off the last 6 chars to drop the java time zone format
        dt_value = dt.datetime.strptime(aStr[:-6], self.TIME_FORMAT)
        return dt_value

    def map_items_to_plot_color(self, items, formats):

        # color_items = [x for x in items if x in self.get_prereq_IDs() or x in self.get_scheduled_test_list()]
        extend = len(items) / len(formats)
        color_list = formats + extend * formats
        color_map = dict(zip(items, color_list))
        return color_map

    def plot_test_timeline(self):
        '''
        plots a timeline of when each box was testing or serving as safety set member
        :return:
        color_map: a dict of prereq_id as keys and color as values
        '''
        sorted_ref_names = self.get_sorted_box_list()

        # check if boxes have been assigned y-axis values yet:
        if any([self.test_set_dict[x][y][VALUE] is None
                for x in self.test_set_dict.keys()
                for y in self.test_set_dict[x].keys()]) or \
                any([self.safety_set_dict[x][y][VALUE] is None
                     for x in self.safety_set_dict.keys()
                     for y in self.safety_set_dict[x].keys()]):
            self.map_yaxis(sorted_ref_names)

        # format plot:
        color_map = self.map_items_to_plot_color(self.get_prereq_IDs(), COLORS_ANY)
        plt.yticks(range(1, 1+ len(sorted_ref_names)), sorted_ref_names)
        plt.ylim(0, 2+ len(sorted_ref_names))
        plt.xlim(self.TEST_START, self.TEST_END + dt.timedelta(minutes=15.0))
        plt.xlabel('Timezone = UTC')
        plt.grid(b=True, which='major', axis='both', color='#CCCCCC', linestyle='-', zorder=0)
        plt.title('Test Set Timeline: ' + self.TEST_LOG_FILENAME.rstrip('.txt'))

        for box in sorted_ref_names:
            # Plot all instances of this box acting as a safety set member:
            try:
                self.safety_set_dict[box]
            except KeyError:
                pass
            else:
                for prereq in self.safety_set_dict[box].keys():
                    plt.plot(self.safety_set_dict[box][prereq][TIME],
                             self.safety_set_dict[box][prereq][VALUE] * len(self.safety_set_dict[box][prereq][TIME]),
                             color_map[prereq], linewidth=1.0)
                    # Plot prereq validity data, if it has been set:
                    try:
                        self.safety_set_dict[box][prereq][VALID_TIME]
                    except KeyError:
                        pass
                    else:
                        plt.plot(self.safety_set_dict[box][prereq][VALID_TIME],
                                 self.safety_set_dict[box][prereq][VALUE] * len(self.safety_set_dict[box][prereq][VALID_TIME]),
                                 color_map[prereq], marker='o', mec=color_map[prereq], markersize=3.0)
            #  Plot all instances of this box as a test set member:
            try:
                self.test_set_dict[box]
            except KeyError:
                pass
            else:
                for test in self.test_set_dict[box].keys():
                    if len(self.test_set_dict[box][test][TIME]) > 0:
                        plt.plot(self.test_set_dict[box][test][TIME],
                                 self.test_set_dict[box][test][VALUE] * len(self.test_set_dict[box][test][TIME]),
                                 'k,')
                        # Plot final test result, if it has been set:
                        try:
                            self.test_set_dict[box][test][RESULT_TIME]
                        except KeyError:
                            pass
                        else:
                            plt.plot(self.test_set_dict[box][test][RESULT_TIME], self.test_set_dict[box][test][VALUE],
                                     color=self.RESULT_FORMAT[self.test_set_dict[box][test][RESULT_VALUE]]['color'],
                                     marker=self.RESULT_FORMAT[self.test_set_dict[box][test][RESULT_VALUE]]['marker'],
                                     markersize=5.0)
                        plt.text(self.test_set_dict[box][test][TIME][0], self.test_set_dict[box][test][VALUE][0]+0.05, test, fontsize=7)
                    else:  # plot 'could not run' tests as yellow
                        plt.plot(self.TEST_END, self.test_set_dict[box][test][VALUE],
                                 color='#ffcf12', marker='^', markersize=5.0)
                        plt.text(self.TEST_END, self.test_set_dict[box][test][VALUE][0], test, fontsize=7)

        plt.savefig('timeline_' + self.TEST_LOG_FILENAME.rstrip('.txt') + '.png')
        plt.show()
        plt.clf()
        return color_map

    def map_yaxis(self, all_boxes_ordered_list):
        box_counter = 0
        for box in all_boxes_ordered_list:
            box_counter += 1
            instance_counter = 0
            num_instances = 0

            try:
                self.test_set_dict[box]
            except KeyError:
                pass
            else:
                num_instances += len(self.test_set_dict[box].keys())
            try:
                self.safety_set_dict[box]
            except KeyError:
                pass
            else:
                num_instances += len(self.safety_set_dict[box].keys())
            step = (Y_TICK_HI - Y_TICK_LO)/(num_instances + 1)

            try:
                self.test_set_dict[box]
            except KeyError:
                pass
            else:
                for test in self.test_set_dict[box].keys():
                    instance_counter += 1
                    self.test_set_dict[box][test][VALUE] = [instance_counter * step + box_counter + Y_TICK_LO]
            try:
                self.safety_set_dict[box]
            except KeyError:
                pass
            else:
                for prereq in self.safety_set_dict[box].keys():
                    instance_counter += 1
                    self.safety_set_dict[box][prereq][VALUE] = [instance_counter * step + box_counter + Y_TICK_LO]

    def plot_test_count(self):
        # tests_only = self.get_scheduled_test_list()
        # tests_only.remove('all')
        # color_map = self.map_items_to_plot_color(tests_only, COLORS)
        color_map = self.map_items_to_plot_color(self.get_scheduled_test_list(), COLORS_ANY)


        text_label_coords = []

        counter = 0
        for test in color_map.keys():

            if test == 'all':
                plt.plot(self.test_count_dict['all'][TIME], self.test_count_dict['all'][VALUE], 'k-', linewidth=2.0)
            else:
                plt.plot(self.test_count_dict[test][TIME], self.test_count_dict[test][VALUE], color_map[test])
            try:
                text_label_val = max(self.test_count_dict[test][VALUE])
                text_label_index = self.test_count_dict[test][VALUE].index(text_label_val)
                text_label = test
            except IndexError:
                text_label_index = 0
                text_label = test + ' could not run'
            text_label_x = self.test_count_dict[test][TIME][text_label_index]
            text_label_y = self.test_count_dict[test][VALUE][text_label_index] + 0.1
            y_spacer = 0
            if [text_label_x, text_label_y] in text_label_coords:
                counter += 1
                y_spacer += 0.5*counter
            text_label_y += y_spacer
            text_label_coords.append([text_label_x, text_label_y])
            plt.text(text_label_x, text_label_y, text_label, fontsize=10)

        # format + save plot:
        plt.ylim(0, MAX_SIMUL_TESTS + 2)
        plt.title('Test Count: ' + self.TEST_LOG_FILENAME.rstrip('.txt'))
        plt.savefig('test count_' + self.TEST_LOG_FILENAME.rstrip('.txt') + '.png')
        plt.show()
        plt.clf()

    def set_prereq_validity_data(self, filename, prereq_ID):
        """
        This method allows the user to associate a plaintext file to a prereq ID.
        1. read file
        2. parse file
        3. save data into dict that was initialized when class was instantiated
        :param filename:
        :param prereq_ID:
        :return:
        """
        prereq_IDs = self.get_prereq_IDs()
        if prereq_ID not in prereq_IDs:
            raise ValueError('PrereqID not recognized')

        with open(filename,'r',0) as f:
            prereq_log_list = f.read().splitlines()

        validity = [int(x) for x in prereq_log_list if x == '0' or x == '1']
        COV_datetime = [dt.datetime.strptime(x.rstrip('"').lstrip('"')[:-5], self.PREREQ_TIME_FORMAT)
                        for x in [x for x in prereq_log_list if 'Z' in x]]

        for box in self.safety_set_dict.keys():
            if prereq_ID in self.safety_set_dict[box].keys():
                self.safety_set_dict[box][prereq_ID][VALID_TIME] = []

                for i in range(len(validity)):
                    if validity[i] == 1 and i+1 < len(validity):
                        self.safety_set_dict[box][prereq_ID][VALID_TIME].extend(
                            [x for x in self.safety_set_dict[box][prereq_ID][TIME]
                             if COV_datetime[i] < x < COV_datetime[i+1]])
                    elif validity[i] == 1 and i+1 >= len(validity):
                        self.safety_set_dict[box][prereq_ID][VALID_TIME].extend(
                            [x for x in self.safety_set_dict[box][prereq_ID][TIME]
                             if COV_datetime[i] < x])

    def set_test_result(self, filename, box, test):
        """
        This method allows the user to set a test result log to a box and test.
        In the future, user should be able to enter a bunch of these at once (as 3-tuples?)
        The result log dict is initialized when the object is defined, and the dict's keys are populated with
         get_test_set method.  The result log dict is left empty of values until set_test_result is called.
        :param filename:
        :param box:
        :param test:
        :return:
        """

        if box not in self.get_scheduled_box_list():
            raise ValueError('Box ref name not recognized')
        elif test not in self.get_scheduled_test_list():
            raise ValueError('Test type not recognized')

        with open(filename,'r',0) as f:
            test_log = f.read().splitlines()

        result_entry = [x for x in test_log if self.TEST_MESSAGE[0] in x or self.TEST_MESSAGE[1] in x]
        result_segments = result_entry[0].split(' - ')

        self.test_set_dict[box][test][RESULT_TIME] = dt.datetime.strptime(result_segments[1][:-6], self.TIME_FORMAT)
        self.test_set_dict[box][test][RESULT_VALUE] = \
            result_segments[2].lstrip(self.TEST_MESSAGE[0]).lstrip(self.TEST_MESSAGE[1])


# some_test = TestSet("valencia-153.txt", "v1.1")  # test class initiation
# some_test.plot_test_timeline()
# some_test.plot_test_count())
# some_test.set_prereq_validity_data('pamf-1472_cdp_fl1.txt', 'ColdDuctPressure 3678')
# some_test.set_prereq_validity_data('pamf-1472_cdp_fl2.txt', 'ColdDuctPressure 3674')
#some_test.set_prereq_validity_data('pamf-1472_hwp.txt', 'HotWaterPressure 3676')
#some_test.set_prereq_validity_data('pamf-1472_hwt.txt', 'HotWaterTemperature 3677')
#some_test.set_prereq_validity_data('pamf-1472_cdt_fl1.txt', 'ColdDuctTemperature 3679')
# some_test.set_prereq_validity_data('pamf-1472_cdt_fl2.txt', 'ColdDuctTemperature 3675')
# some_test.plot_test_timeline()
# some_test.set_test_result('pamf-1472_vav2-6_dpc.txt', '#pdc_vav_2_6_VAVR_site_97', 'VVR_DPC')
# some_test.set_test_result('pamf-1472_vav2-6_cool.txt', '#pdc_vav_2_6_VAVR_site_97', 'VVR_ZSA')
# some_test.set_test_result('pamf-1472_vav1-12_afs.txt', '#pdc_vav_1_12_VAVR_site_97', 'VVR_AFS')
# some_test.plot_test_timeline()
"""
TODO:

make plotting faster!
print test set stats: elapsed time, avg runtime per test, avg dead time, (what else?)
print log of any other issues (locked zones, what else?)
force better numeric sorting on ref names
format time axis
thematically color prereqs
put labels at the start of each line for prereqs
add ability to plot only one box, or one prereq, or one test
DONE:
refactor! streamline process so only run through log once.  get__ methods can print keys of dicts or return lists from main fn - done
for any test that was scheduled to run but never appeared in running = [ by the end of the test set, mark as "could not run" - done
add ability for user to set individual test log and plot test result(green, red, yellow) at date_time that result is assigned. - done
incorporate data availability into state machine status data - done
graph number of simultaneous tests occuring at the time - done
convert to class structure - done
be resilient when prereqs are run manually - done
make sure multiple prereqs don't plot on top of themselves - done
plot gridlines - done
put box names on y axis ticks - done
use boxes as keys for both dicts - done
"""
