#!/usr/bin/python

import sys
from  WorkloadGenerator import *


def main():
    if len(sys.argv) > 1:
        output_filename = sys.argv[1]
    else:
        output_filename = "workload.wkld"

    print "Generating workload..."

    model = Model()
    pubsubgen = PubSubGenerator() 
    pubsubgen.set_dist_file_type("types.dist")
    pubsubgen.set_dist_file_attr_name("words_freq_top_100")
    pubsubgen.set_dist_file_constr_name("words_freq_top_100")
    pubsubgen.set_dist_file_string("words_freq_top_100")
    pubsubgen.set_dist_file_string_ops("string_operators.dist")
    pubsubgen.set_dist_file_int("int_values.dist")
    pubsubgen.set_dist_file_numerical_ops("operators.dist")
    pubsubgen.set_dist_file_bool("bool_values.dist")

    # Subscriber 
    t_actor = Actor("S1C1")
    """
    f = generate_filters(min_constr=1, max_constr=2, count=1, dfile_type="types.dist",\
        dfile_constr_name="words_freq_top_4",\
        dfile_string="words_freq_top_4",\
        dfile_string_operator="string_operators.dist",\
        dfile_int="int_values.dist", dfile_numeric_operator="operators.dist",\
        dfile_bool="bool_values.dist")
    t_actor.add_filters(f)
    f = generate_filters(min_constr=1, max_constr=1, count=1, dfile_type="types.dist",\
        dfile_constr_name="words_freq_top_3",\
        dfile_string="words_freq_top_3",\
        dfile_string_operator="string_operators.dist",\
        dfile_int="int_values.dist", dfile_numeric_operator="operators.dist",\
        dfile_bool="bool_values.dist")
    t_actor.add_filters(f)
    """
    f = pubsubgen.generate_filters(min_constr=1, max_constr=5, count=10)
    t_actor.add_filters(f)
    model.add_actor(t_actor)

    # High-rate publisher
    actor = Actor("S2C1")
    # Steady low
    t = generate_timestamps_equal_periods(70, 30, [40])
    # Ramp
    t.extend(generate_timestamps_equal_periods(100, 60, range(40, 400, 60)))
    # Steady
    t.extend(generate_timestamps_equal_periods(160, 60, [400]))
    # Bursty
    t.extend(generate_timestamps_bursty(begin_secs=220, duration_secs=100,\
        normal_rate_per_secs=80,\
        min_burst_rate_per_secs=150, max_burst_rate_per_secs=250,\
        min_burst_length_millisecs=500, max_burst_length_millisecs=1000,\
        num_of_bursts=20))
    # customized
    t.extend(generate_timestamps_custom_periods(320, [(30, 200),(70, 140),(20,50)]))

    p = pubsubgen.generate_publications(min_attr=1, max_attr=5, count=len(t))
    actor.add_publications(t, p)

    # Second catrgory of publications with other keywords
    # Steady 
    t = generate_timestamps_equal_periods(70, 250, [100])
    p = pubsubgen.generate_publications(min_attr=1, max_attr=5, count=len(t))
    actor.add_publications(t, p)

    t = generate_timestamps_equal_periods(70, 250, [50])
    p = pubsubgen.generate_publications(min_attr=1, max_attr=5, count=len(t))
    actor.add_publications(t, p)

    actor.number_of_subscriptions = 0
    model.add_actor(actor)    

    model.set_publication_format("pub {id} {count} {time} {event}")
    model.set_subscription_format("sub {id} {count} {time} {event}")
    model.generate_events(output_filename)
    print "done"

if __name__ == "__main__":
    main()
