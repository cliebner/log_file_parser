__author__ = 'christina'


"""
Started: 5 Oct 2016

Define the most abstract log file with time-dependent data.

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
* message pertaining to a specific (user defined by user input?) spc
* broadcast message
* sent messages
* received messages
* same IP address
* within a single session
* on the same day
* AT commands

(custom datetimeFixer) Enforce dates:
1. scan file, look for 00000 and grab dates
2. compare timestamp right before first 0000 (t-1)
3. if t-1 is > t at 0000, then assume it's from the day before, and assign to date of first 00000 minus 1 day
4. hold onto date stamp from 0000 until next 00000 (or end of file) shows up, then replace date stamp with next one (or stop reading file)

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

class abstractTimeLogReader():
    pass

def abstractDateTimeFixer():
    # apply user-defined date time fixer method to the lines in the read-in file
    pass

class abstractBucket():
    # do in a loop based on user input?
    # newBucketClass = bucketClassFactory()
    pass

def bucketFactory():
    # make buckets based on user's rules! in a json perhaps??
    pass