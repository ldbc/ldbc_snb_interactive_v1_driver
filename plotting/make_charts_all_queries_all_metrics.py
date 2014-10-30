import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.font_manager import FontProperties
import json
import sys

arg_count = len(sys.argv)-1
if arg_count != 2:
	print "2 parameter expected (input_file, legend_location left/center/right/none) - %s given"%arg_count
	exit()

filename = sys.argv[1]
legend_location = sys.argv[2]

# MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx512m" mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-db,com.ldbc.socialnet.workload.neo4j.Neo4jDb,-w,com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload,-oc,10,-rc,-1,-tc,1,-s,-tu,MILLISECONDS,-p,neo4j.path=db/,-p,neo4j.dbtype=embedded-api-steps,-p,parameters=ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json"
# MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx512m" mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-P,ldbc_socnet_interactive.properties"

json_data=open(filename).read()
all_results = json.loads(json_data)
query_data = all_results["all_metrics"]
query_data = sorted(query_data, key=lambda query: int(query["name"][ query["name"].rfind("y")+1 : ]))
# for query in query_data:
# 	print str(query["name"]) + "    " + str(query["run_time"]["95th_percentile"])

time_min = [query["run_time"]["min"] for query in query_data]
time_50 = [query["run_time"]["50th_percentile"] for query in query_data]
time_90 = [query["run_time"]["90th_percentile"] for query in query_data]
time_99 = [query["run_time"]["99th_percentile"] for query in query_data]
time_max = [query["run_time"]["max"] for query in query_data]

query_names = [query["name"].split(".")[-1] for query in query_data]

# ind = np.arange(len(query_names))  # the x locations for the groups
ind = np.arange(0, 2*len(query_names), 2) 
w = 0.34       # the width of the bars

fig, ax = plt.subplots()

rects_min = ax.bar(ind, time_min, width=w, color='c', align='center', log=True)
rects_50 = ax.bar(ind+w, time_50, width=w, color='g', align='center', log=True)
rects_90 = ax.bar(ind+w+w, time_90, width=w, color='r', align='center', log=True)
rects_99 = ax.bar(ind+w+w+w, time_99, width=w, color='y', align='center', log=True)
rects_max = ax.bar(ind+w+w+w+w, time_max, width=w, color='m', align='center', log=True)

time_unit = all_results["unit"]
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
	ax.legend( (rects_min[0], rects_50[0], rects_90[0], rects_99[0], rects_max[0]), ('min', '50th', '90th', '99th', 'max'),
		loc='upper %s'%legend_location, fancybox=True, shadow=False, ncol=1, prop=fontP)#, bbox_to_anchor=(1.2, 1.0))

# attach some text labels
def autolabel(rects):
    for rect in rects:
		height = rect.get_height()
		if height == 0:
			ax.text(rect.get_x()+rect.get_width()/2., 0.8, '%d'%int(height), ha='center', va='top', rotation=90, size='small')
		else:
			ax.text(rect.get_x()+rect.get_width()/2., 0.9*height, '%d'%int(height), ha='center', va='top', rotation=90, size='small')

autolabel(rects_min)
autolabel(rects_50)
autolabel(rects_90)
autolabel(rects_99)
autolabel(rects_max)

# ax.autoscale_view()

# plt.xlim([0,len(query_names)])
y_upper = 1.1 * max(time_max)
plt.ylim(ymax = y_upper, ymin = 0)
# plt.axis('tight')
plt.margins(0.05, 0.0)
# SET IMAGE SIZE - USEFUL FOR OUTSIDE LEGEND
# plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.14)
# plt.tight_layout()
plt.ylim(ymin = 0.1)
plt.savefig('result.pdf')
plt.show()
