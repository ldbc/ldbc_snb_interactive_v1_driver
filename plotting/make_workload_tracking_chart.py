import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.font_manager import FontProperties
import json
import sys


from pylab import plotfile, show, gca
import matplotlib.cbook as cbook


data = np.genfromtxt('/Users/alexaverbuch/IdeaProjects/scale_factor_streams/sf_10_partitions_02/TC2-sf_10_partitions_02-results_log.csv', delimiter='|', skip_header=1,
                     skip_footer=0, names=['operation_type', 'scheduled_start_time', 'actual_start_time', 'execution_duration'])

operation_counts = range(1,len(data)+1)
operation_types = list(data['operation_type'])
scheduled_start_times = list(data['scheduled_start_time'])
actual_start_times = list(data['actual_start_time'])

print operation_types


# red dashes, blue squares and green triangles
plt.plot(operation_counts, actual_start_times, 'r--')
plt.show()




# fig = plt.figure()

# ax1 = fig.add_subplot(111)

# ax1.set_title("Workload Tracking")    
# ax1.set_xlabel('Operation Count')
# ax1.set_ylabel('Time')

# ax1.plot(operation_counts,scheduled_start_times, actual_start_times, c='r', label='the data')

# leg = ax1.legend()

# plt.show()

# from pylab import plotfile, show, gca
# import matplotlib.cbook as cbook

# fname = cbook.get_sample_data('msft.csv', asfileobj=False)
# fname2 = cbook.get_sample_data('data_x_x2_x3.csv', asfileobj=False)

# # test 1; use ints
# plotfile(fname, (0,5,6))

# # test 2; use names
# plotfile(fname, ('date', 'volume', 'adj_close'))

# # test 3; use semilogy for volume
# plotfile(fname, ('date', 'volume', 'adj_close'), plotfuncs={'volume': 'semilogy'})

# # test 4; use semilogy for volume
# plotfile(fname, (0,5,6), plotfuncs={5:'semilogy'})

# #test 5; single subplot
# plotfile(fname, ('date', 'open', 'high', 'low', 'close'), subplots=False)

# # test 6; labeling, if no names in csv-file
# plotfile(fname2, cols=(0,1,2), delimiter=' ',
#          names=['$x$', '$f(x)=x^2$', '$f(x)=x^3$'])

# # test 7; more than one file per figure--illustrated here with a single file
# plotfile(fname2, cols=(0, 1), delimiter=' ')
# plotfile(fname2, cols=(0, 2), newfig=False, delimiter=' ') # use current figure
# gca().set_xlabel(r'$x$')
# gca().set_ylabel(r'$f(x) = x^2, x^3$')

# # test 8; use bar for volume
# plotfile(fname, (0,5,6), plotfuncs={5:'bar'})

# show()






# arg_count = len(sys.argv)-1
# if arg_count != 3:
# 	print "3 parameters expected (input_file_a, input_file_b, legend_location left/center/right/none) - %s given"%arg_count
# 	exit()

# filename_1 = sys.argv[1]
# filename_2 = sys.argv[2]
# legend_location = sys.argv[3]

# json_data_a=open(filename_1).read()
# json_data_b=open(filename_2).read()

# query_data_a = json.loads(json_data_a)["all_metrics"]
# query_data_a = sorted(query_data_a, key=lambda query: int(query["name"][ query["name"].rfind("y")+1 : ]))
# query_data_b = json.loads(json_data_b)["all_metrics"]
# query_data_b = sorted(query_data_b, key=lambda query: int(query["name"][ query["name"].rfind("y")+1 : ]))

# time_90_a = [query["run_time"]["90th_percentile"] for query in query_data_a]
# time_90_b = [query["run_time"]["90th_percentile"] for query in query_data_b]
# time_50_a = [query["run_time"]["50th_percentile"] for query in query_data_a]
# time_50_b = [query["run_time"]["50th_percentile"] for query in query_data_b]

# query_names = [query["name"].split(".")[-1] for query in query_data_a]

# ind = np.arange(0, 2*len(query_names), 2) 
# w = 0.34       # the width of the bars

# fig, ax = plt.subplots()

# rects_90_a = ax.bar(ind, time_90_a, width=w, color='green', align='center', log=True, hatch='/')
# rects_90_b = ax.bar(ind+w, time_90_b, width=w, color='yellow', align='center', log=True, hatch='/')
# rects_50_a = ax.bar(ind+w+w, time_50_a, width=w, color='green', align='center', log=True)
# rects_50_b = ax.bar(ind+w+w+w, time_50_b, width=w, color='yellow', align='center', log=True)

# time_unit = query_data_a[0]["run_time"]["unit"]
# ax.set_ylabel('Runtime (%s)'%time_unit)
# ax.set_title('LDBC SNB Interactive')
# ax.set_xticks(ind+w+w+w/2)
# ax.set_xticklabels(tuple(query_names))
# ax.set_yscale('symlog')
# ax.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
# # ax.xaxis_date()
# # ax.autoscale(tight=True)

# fig.autofmt_xdate()

# fontP = FontProperties()
# fontP.set_size('small')
# if legend_location != "none":
# 	ax.legend( (rects_90_a[0], rects_90_b[0], rects_50_a[0], rects_50_b[0],), ('90th a', '90th b', '50th a', '50th b'),
# 		loc='upper %s'%legend_location, fancybox=True, shadow=False, ncol=1, prop=fontP)#, bbox_to_anchor=(1.2, 1.0))

# # attach some text labels
# def autolabel(rects):
#     for rect in rects:
# 		height = rect.get_height()
# 		if height == 0:
# 			ax.text(rect.get_x()+rect.get_width()/2., 0.8, '%d'%int(height), ha='center', va='top', rotation=90, size='small')
# 		else:
# 			ax.text(rect.get_x()+rect.get_width()/2., 0.9*height, '%d'%int(height), ha='center', va='top', rotation=90, size='small')

# autolabel(rects_90_a)
# autolabel(rects_90_b)

# # ax.autoscale_view()

# # plt.xlim([0,len(query_names)])
# y_upper = 1.1 * max(max(time_90_a),max(time_90_b))
# plt.ylim(ymax = y_upper, ymin = 0)
# # plt.axis('tight')
# plt.margins(0.05, 0.0)
# # SET IMAGE SIZE - USEFUL FOR OUTSIDE LEGEND
# # plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.14)
# # plt.tight_layout()
# plt.ylim(ymin = 0.1)
# plt.savefig('result_compare.pdf')
# plt.show()