import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.font_manager import FontProperties
import json
import sys

arg_count = len(sys.argv)-1
if arg_count != 5:
	print "5 parameters expected (input_file_a, name_a, input_file_b, name_b legend_location left/center/right/none) - %s given"%arg_count
	exit()

filename_1 = sys.argv[1]
name_a = sys.argv[2]
filename_2 = sys.argv[3]
name_b = sys.argv[4]
legend_location = sys.argv[5]

json_data_a=open(filename_1).read()
json_data_b=open(filename_2).read()

query_data_a = json.loads(json_data_a)["all_metrics"]
query_data_a = sorted(query_data_a, key=lambda query: int(query["name"][ query["name"].rfind("y")+1 : ]))
query_data_b = json.loads(json_data_b)["all_metrics"]
query_data_b = sorted(query_data_b, key=lambda query: int(query["name"][ query["name"].rfind("y")+1 : ]))

time_95_a = [query["run_time"]["95th_percentile"] for query in query_data_a]
time_95_b = [query["run_time"]["95th_percentile"] for query in query_data_b]
time_mean_a = [query["run_time"]["mean"] for query in query_data_a]
time_mean_b = [query["run_time"]["mean"] for query in query_data_b]

query_names = [query["name"].split(".")[-1] for query in query_data_a]

ind = np.arange(0, 2*len(query_names), 2) 
w = 0.34       # the width of the bars

fig, ax = plt.subplots()

rects_95_a = ax.bar(ind, time_95_a, width=w, color='green', align='center', log=True, hatch='/')
rects_95_b = ax.bar(ind+w, time_95_b, width=w, color='yellow', align='center', log=True, hatch='/')
rects_mean_a = ax.bar(ind+w+w, time_mean_a, width=w, color='green', align='center', log=True)
rects_mean_b = ax.bar(ind+w+w+w, time_mean_b, width=w, color='yellow', align='center', log=True)

time_unit = query_data_a[0]["run_time"]["unit"]
ax.set_ylabel('Runtime (%s)'%time_unit)
ax.set_title('LDBC SNB Interactive')
ax.set_xticks(ind+w+w+w/2)
ax.set_xticklabels(tuple(query_names))
ax.set_yscale('symlog')
ax.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
# ax.xaxis_date()
# ax.autoscale(tight=True)

fig.autofmt_xdate()

fontP = FontProperties()
fontP.set_size('small')
if legend_location != "none":
	ax.legend( (rects_95_a[0], rects_95_b[0], rects_mean_a[0], rects_mean_b[0],), ('95th '+ name_a, '95th ' + name_b, 'mean ' + name_a, 'mean ' + name_b),
		loc='upper %s'%legend_location, fancybox=True, shadow=False, ncol=1, prop=fontP)#, bbox_to_anchor=(1.2, 1.0))

# attach some text labels
def autolabel(rects):
    for rect in rects:
		height = rect.get_height()
		if height == 0:
			ax.text(rect.get_x()+rect.get_width()/2., 0.8, '%d'%int(height), ha='center', va='top', rotation=90, size='small')
		else:
			ax.text(rect.get_x()+rect.get_width()/2., 0.9*height, '%d'%int(height), ha='center', va='top', rotation=90, size='small')

autolabel(rects_95_a)
autolabel(rects_95_b)

# ax.autoscale_view()

# plt.xlim([0,len(query_names)])
y_upper = 1.1 * max(max(time_95_a),max(time_95_b))
plt.ylim(ymax = y_upper, ymin = 0)
# plt.axis('tight')
plt.margins(0.05, 0.0)
# SET IMAGE SIZE - USEFUL FOR OUTSIDE LEGEND
# plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.14)
# plt.tight_layout()
plt.ylim(ymin = 0.1)
plt.savefig('result_compare.pdf')
plt.show()