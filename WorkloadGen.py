#  WorkloadGen
#
#  Author: Amir Malekpour
#  Copyright (C) Amir Malekpour
#
#  WorkloadGen is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with Siena.  If not, see <http:#www.gnu.org/licenses/>.
#

from operator import itemgetter, attrgetter
import random
import bisect
import math
import sys


global _flag_matplitlib_installed
_flag_matplitlib_installed = True

# try importing pyplot from matplotlib
# this adds the optional feature of plotting 
# some workload information
try:
    from matplotlib import pyplot
except ImportError:
    _flag_matplitlib_installed = False

def plot_events(ts, fname):
    if not _flag_matplitlib_installed:
        return
    step = 1000
    begin = ts[0]
    time = []
    counts = []
    index = 0
    end = 0
    while end < ts[-1]:
        counter = 0
        end = begin + step
        while end > ts[index]:
            counter += 1
            index += 1
        time.append(end)
        counts.append(counter)
        begin = begin + step

    pyplot.plot(time, counts)
    pyplot.savefig(fname)
    pyplot.close()

#=======================================================================
# The class Model is a container for a set of Actors.
#=======================================================================
class Model:
    def __init__(self):
        self.actors = {}
        self.events = []
        self._separator = ","
        self._flag_indicate_type = False
        self._str_pub_format = None
        self._str_sub_format = None

    def add_actor(self, ac_id, ac_instance):
        self.actors[ac_id] = ac_instance

    def add_actor(self, ac_instance):
        self.actors[ac_instance.id] = ac_instance

    def generate_events(self, fname):
        print("Generating events for %s actors..." %len(self.actors))
        percent = float(len(self.actors)) / 10 # percent work per actor
        counter = 0
        for tmp_id, tmp_intance in self.actors.items():
            print(tmp_id)
            tmp_intance.generate_events(self.events,\
                f_str_sub = self._str_sub_format,\
                f_str_pub = self._str_pub_format)
            """
            counter += 1
            if( (counter * percent) % 10 == 0):
                print("%s0%%" %(counter*tenth)/10)
            """
        print("Writing workload file %s ..."%fname)
        # Sort events based on time
        self.events.sort(key=itemgetter(0))
        f = open(fname, 'w')
        f.write(self.events[0][1])
        for item in self.events[1:]:
            f.write("\n" + item[1])
        f.close()
        print("done.")

    def plot_events(self, fname):
        plot_events(self.events, fname)

    def set_separator(s):
        self._separator = s

    def set_publication_format(self, str):
        self._str_pub_format = str

    def set_subscription_format(self, str):
        self._str_sub_format = str

    def indicate_type(f):
        self._flag_indicate_type = f

#=======================================================================
# This class generates filters and messages (publications and subscriptions)
# The user first set the necessary distribution files by calling
# 'set_dist_file_*' and then calling 'generate_publications' and 
# 'generate_filters'. 
#=======================================================================
class PubSubGenerator:
    def __init__(self):
        self._token_generators = dict()
        self._token_generators["type"] = WeightedItemSelector(None, "type")
        self._token_generators["attr_name"] = WeightedItemSelector(None, "attribute name")
        self._token_generators["constr_name"] = WeightedItemSelector(None, "constraint name")
        self._token_generators["string"] = WeightedItemSelector(None, "string")
        self._token_generators["str_ops"] = WeightedItemSelector(None, "string operators")
        self._token_generators["int"] = WeightedItemSelector(None, "integer values")
        self._token_generators["double"] = WeightedItemSelector(None, "double values")
        self._token_generators["num_ops"] = WeightedItemSelector(None, "numerical operators")
        self._token_generators["bool"] = WeightedItemSelector(None, "boolean values")

    def set_dist_file_type(self, fname):
        self._token_generators["type"] = WeightedItemSelector(fname, "type")
       
    def set_dist_file_attr_name(self, fname):
        self._token_generators["attr_name"] = WeightedItemSelector(fname, "attribute name")

    def set_dist_file_constr_name(self, fname):
        self._token_generators["constr_name"] = WeightedItemSelector(fname, "constraint name")

    def set_dist_file_string(self, fname):
        self._token_generators["string"] = WeightedItemSelector(fname, "string")
    
    def set_dist_file_string_ops(self, fname):
        self._token_generators["str_ops"] = WeightedItemSelector(fname, "string operators")
    
    def set_dist_file_int(self, fname):
        self._token_generators["int"] = WeightedItemSelector(fname, "integer values")
 
    def set_dist_file_double(self, fname):
        self._token_generators["double"] = WeightedItemSelector(fname, "double values")
       
    def set_dist_file_numerical_ops(self, fname):
        self._token_generators["num_ops"] = WeightedItemSelector(fname, "numerical operators")

    def set_dist_file_bool(self, fname):
        self._token_generators["bool"] = WeightedItemSelector(fname, "boolean values")
    
    def generate_publications(self, count=1, min_attr=1, max_attr=1):
        result = [] 
        # get the requireed token generators
        token_generator_type = self._token_generators["type"]    
        token_generator_attr_name = self._token_generators["attr_name"]    
        token_generator_string = self._token_generators["string"]
        token_generator_int = self._token_generators["int"]
        token_generator_double = self._token_generators["double"]
        token_generator_bool = self._token_generators["bool"]
        token_generator_string_operator = self._token_generators["str_ops"]
        token_generator_numeric_operator = self._token_generators["num_ops"]
       
        # Start generation process
        pub_counter = 0
        names = set()
        for i in range(count):
            str_atts = ""
            names.clear()
            j = 0
            while j < random.randint(min_attr, max_attr):
                # first determine the type
                type = token_generator_type.get_item()
                attr_name = token_generator_attr_name.get_item()
                if attr_name in names:
                    continue
                names.add(attr_name)
                if type == "s" :
                    attr_value = token_generator_string.get_item()
                elif type == "i" :
                    attr_value = token_generator_int.get_item()
                elif type == "b":
                    attr_value = token_generator_bool.get_item()
                else:
                    print "Unknown event type: %s" %type
                    continue
                j += 1
                str_atts = str_atts + "%s %s %s , " %(type, attr_name, attr_value)
            str_atts = str_atts[:-3]
            result.append(str_atts)
            pub_counter += 1
            # return the number scheduled publish events for this client
        return result


    def generate_filters(self, count=1, min_constr=1, max_constr=1):
        result = []

        # get the requireed token generators
        token_generator_type = self._token_generators["type"]    
        token_generator_constr_name = self._token_generators["constr_name"]    
        token_generator_string = self._token_generators["string"]
        token_generator_int = self._token_generators["int"]
        token_generator_double = self._token_generators["double"]
        token_generator_bool = self._token_generators["bool"]
        token_generator_string_operator = self._token_generators["str_ops"]
        token_generator_numeric_operator = self._token_generators["num_ops"]

        # Start generation process
        names = set()
        for i in range(count):
            names.clear()
            temp_str = ""
            j = 0
            while j< random.randint(min_constr, max_constr):
                # first determine the type
                type = token_generator_type.get_item()
                const_name = token_generator_constr_name.get_item()
                if const_name in names: 
                    continue
                names.add(const_name)
                if type == "s":
                    const_value = token_generator_string.get_item()
                    op = token_generator_string_operator.get_item()
                elif type == "i" :
                    const_value = token_generator_int.get_item()
                    op = token_generator_numeric_operator.get_item()
                elif type == "b":
                    const_value = token_generator_bool.get_item()
                    op = "="
                else:
                    print "Unknown event type: %s" %type
                    continue
                j += 1
                temp_str += "%s %s %s %s , " %(type, const_name, op, const_value)
            temp_str = temp_str[:-3]
            result.append(temp_str)
        return result

#=======================================================================
# This class gets the name of a distribution file that lists items
# along with their weights and returns an item when get_item is called
# based on its assigned weight
#=======================================================================
class WeightedItemSelector:
    def __init__(self, file_name, desc):
        self._inited = False
        self._desc = desc
        if file_name == None:
            return
        try:
            f = open(file_name, 'r')
            temp_list = []
            total_weight = 0
            for line in f:
                line = line.strip("\r\n")
                line = line.strip()
                if line == "" or line[0] == "#" : continue
                freq, word = line.split(" ")
                if freq == "0" : continue
                temp_list.append( (word, int(freq)) )
                total_weight += int(freq)
            f.close()
            self._inited = True
        except IOError:
            print "\nError: distribution file for %s does not exist: %s"%(self._desc, file_name)
            exit(-1)
     
        # sort the list based on the weights, ascending ordered
        temp_list.sort(key=itemgetter(1))

        # Make a second list and insert word with their normalized weights
        self. word_list_normalized = []
        t = 0
        for item in temp_list:
            t += float(item[1])/total_weight
            self.word_list_normalized.append( (item[0], t) )
        
        # Create an array of weights and sort the array so that the array
        # and the list of word/weights are sorted the same way
        self.weights = [r[1] for r in self.word_list_normalized]
        self.weights.sort()

    def get_item(self):
        if not self._inited:
            print "\nError: distribution file name for %s is not specified.\n"%sself._desc
            exit(-1)
        index =  random.random()
        item = self.word_list_normalized[bisect.bisect_left(self.weights, index)][0]
        return item

#========================================================================
# The following class implements Actor, which is a publisher/subscriber   
#========================================================================
class Actor():
    def __init__(self, id):
        self.id = id
        self.events = []
        #################### Begin attributes ##########################
        # NOTE: If you add or remove any property here, don't forget
        # to modify method "copy_parameters_from()" accodringly. 

        self.subscribe_start_time = 30000
        self.publish_start_time = 60000
        self.stop_time = 360000
        self.subscription_timeout = 30000 
        self.unsubscription_timeout = 60000
        ######################## End attributes ################################
        self._total_publication_count = 0
        self._total_subscription_count = 0
        self._filters = []
        self._pub_set = []
    
    def clear_filters(self):
        self._filters.clear()

    def generate_events(self, events=None, f_str_sub = None, f_str_pub = None):
        self.generate_sub_events(f_str_sub)
        self.generate_pub_events(f_str_pub)
        events.extend(self.events)
        # Since in Weevil, after the last scheduled event, the client terminates
        # its execution, and does not receive any notification we schedule a
        # "dummy" publish event at the very end of the experiment, which makes
        # the client stay online during the whole course of experiment
        #event = "event(%s,%s,`PUB(%s, `\"%s\"')',,)" %(self.stop_time+10000,\
        #    self.id, self._total_publication_count, "s dummy dummy & s msgId dummy")
        #events.append( (self.stop_time, event) )

    def add_filters(self, filters):
        self._filters.extend(filters)

    def generate_sub_events(self, f_str = None):
        time_t = self.subscribe_start_time + random.randint(0, 5000)
        
        if(f_str == None):
            format_str = "subscribe {id} {count} {time} {event}"
        else:
            format_str = f_str
        for item in self._filters:
            self._total_subscription_count += 1
            event = format_str.format(\
            id=self.id, \
            count=self._total_subscription_count,\
            time = time_t,\
            event = item) 
            #event = "event(%s,%s,`SUB(%s, `\"%s\"')',,)" %(time, self.id,\
            #    self._total_subscription_count, item)
            self.events.append( (time_t, event) )

    def add_publications(self, timestamps, publications):
        if len(timestamps) != len(publications):
            print "Error adding publications for actor %s: timestamps and publications must have the same length." %self.id
            exit -1
        for t, p in zip(timestamps, publications):
            self._pub_set.append((t, p))

    def generate_pub_events(self, f_str = None):
        if self.events == None:
            self.events = []
        if(f_str == None):
            format_str = "publish {id} {count} {time} {event}"
        else:
            format_str = f_str
        self._pub_set.sort(key=itemgetter(0))
        for item in self._pub_set:
            time_t = item[0]
            pub = item[1]
            self._total_publication_count += 1
            event = format_str.format(\
                    id=self.id, \
                    count=self._total_publication_count,\
                    time = time_t,\
                    event = pub)
            self.events.append((time_t, event))
        # return the number scheduled publish events for this client
        return self._total_publication_count

    def copy_parameters_from(self, other):
        
        self.subscribe_start_time = other.subscribe_start_time
        self.publish_start_time = other.publish_start_time
        self.stop_time = other.stop_time
        # I don't know what the following two are for. I put them 
        # here for potential future usage
        self.subscription_timeout = other.subscription_timeout
        self.unsubscription_timeout = other.unsubscription_timeout


#========================= Timestamp generation functions  ====================================

def generate_timestamps_bursty(begin_secs=0, duration_secs=0, normal_rate_per_secs=0,\
    min_burst_rate_per_secs=0, max_burst_rate_per_secs=0,\
    min_burst_length_millisecs=0, max_burst_length_millisecs=0, \
    num_of_bursts=0):
    """ First determine the timestamp of bursts. Insert burstd events and then
    we insert low rate events amongst the bursts """
    ts = []
    bursts = []
    begin_milliseconds = begin_secs * 1000
    end_milliseconds = begin_milliseconds + (duration_secs * 1000)
    for i in range(num_of_bursts):
        # Note: I want the events to start and end  with a normal rate other than a burst
        # rate. Thus here i start bursts at least 1000 milliseconds after and
        # before the total given duration
        burst_time = random.randint(begin_milliseconds + 1000, end_milliseconds - 1000)
        burst_length = random.randint(min_burst_length_millisecs, max_burst_length_millisecs)
        burst_rate =  random.randint(min_burst_rate_per_secs, max_burst_rate_per_secs)
        bursts.append( (burst_time, burst_length, burst_rate) )

    bursts = sorted(bursts, key=itemgetter(0))
    # Now we insert ordinary events among bursts
    tmp_begin = begin_milliseconds
    for burst in bursts:
        # noraml events
        duration = burst[0]  - tmp_begin
        count = duration * normal_rate_per_secs / 1000
        for i in range(count):
            ts.append(random.randint(tmp_begin,burst[0]))
        #bursts events
        tmp_begin = burst[0]
        duration = burst[1]
        tmp_end = tmp_begin + duration
        count = duration * burst[2] / 1000
        for i in range(count):
            ts.append(random.randint(tmp_begin, tmp_end))
        tmp_begin = tmp_end

    duration = end_milliseconds - tmp_begin
    count = duration * normal_rate_per_secs / 1000
    for i in range(count):
        ts.append(random.randint(tmp_begin, end_milliseconds))
    return sorted(ts)

# This function takes a start and end time (in seconds) and a list scalaers
# e.g. [200,100,400,0] each of which speficies the publication rate
# (messages/second) in that period. The experiemt duration is divided to
# equal-length sub-periods, each assigned a rate from the corresponding index
# of the list e.g. the first period's rate is the first element of the list
# etc.
def generate_timestamps_equal_periods(begin_secs=0, duration_secs=0, list_rates_per_second=0):
    timestamps = []
    periods = len(list_rates_per_second)
    period_duration_milliseconds = duration_secs * 1000 / periods
    time_step = min(period_duration_milliseconds, 1000)
    start = begin_secs * 1000 
    for period in range(periods):
        period_end = start + period_duration_milliseconds
        while start < period_end:
            stop = min(start + time_step, period_end)
            sub_period_pub_count = int(math.ceil((stop-start)/1000.0) * \
                list_rates_per_second[period])
            for i in range(sub_period_pub_count):
                timestamps.append(random.randint(start, stop))
            start = stop
    timestamps.sort()
    return timestamps

# This function takes a start time (seconds) and list of tuples, each element
# in the list must be a tuple of two integers e.g.
# [(30,200),(70,140),(400,50)]. Each tuple speficies one sub-period, with the
# first item in tuple being the duration of the sub-period (in seconds) and the
# second item in tuple specifies the publication rate (message/seoncd) in that
# period. The begining of the first sub-periods is specified by the start time
# passed to this function and naturally, the beginings of the susequent
# sub-periods (second, third, ...) is the start time + the sum of the duration
# of the previous periods
def generate_timestamps_custom_periods(start, list_duration_rates_per_secs):
    timestamps = []
    for period in list_duration_rates_per_secs:
        if not isinstance(period, tuple) :
            print("\nThe format of publication_rate_per_second is incorrect.\
                Either a list of numbers or a lisr of tuple i.e., [(end-time,\
                number-of-pubs)...(,)] is expected)")
            exit(-1)
        timestamps.extend(generate_timestamps_equal_periods(start, period[0], [period[1]]))
        start += period[0]
    return timestamps


