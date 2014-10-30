import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.font_manager import FontProperties
import json
import sys

arg_count = len(sys.argv)-1
if arg_count != 3:
	print "WRONG! 3 parameters expected (input_file, metric_to_plot, legend_location left/center/right/none), %s given"%arg_count
	exit()

filename = sys.argv[1]
metric_to_plot = sys.argv[2]
legend_location = sys.argv[3]

# MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx512m" mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-db,com.ldbc.socialnet.workload.neo4j.Neo4jDb,-w,com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload,-oc,10,-rc,-1,-tc,1,-s,-tu,MILLISECONDS,-p,neo4j.path=db/,-p,neo4j.dbtype=embedded-api-steps,-p,parameters=ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json"
# MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx512m" mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-P,ldbc_socnet_interactive.properties"

json_data=open(filename).read()
all_results = json.loads(json_data)
query_data = all_results["all_metrics"]
query_data = sorted(query_data, key=lambda query: int(query["name"][ query["name"].rfind("y")+1 : ]))

metric = [query["run_time"][metric_to_plot] for query in query_data]

query_names = [query["name"].split(".")[-1] for query in query_data]

# ind = np.arange(len(query_names))  # the x locations for the groups
ind = np.arange(0, 2*len(query_names), 2) 
w = 1.0       # the width of the bars

fig, ax = plt.subplots()

rects_metric = ax.bar(ind, metric, width=w, color='c', align='center', log=True)

time_unit = all_results["unit"]
ax.set_ylabel('Runtime (%s)' % time_unit)
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
	ax.legend( (rects_metric[0],), (metric_to_plot,),
		loc='upper %s'%legend_location, fancybox=True, shadow=False, ncol=1, prop=fontP)#, bbox_to_anchor=(1.2, 1.0))

# attach some text labels
def autolabel(rects):
    for rect in rects:
		height = rect.get_height()
		if height == 0:
			ax.text(rect.get_x()+rect.get_width()/2., 0.8, '%d'%int(height), ha='center', va='top', rotation=90, size='small')
		else:
			ax.text(rect.get_x()+rect.get_width()/2., 0.9*height, '%d'%int(height), ha='center', va='top', rotation=90, size='small')

autolabel(rects_metric)

# ax.autoscale_view()

# plt.xlim([0,len(query_names)])
y_upper = 1.1 * max(metric)
plt.ylim(ymax = y_upper, ymin = 0)
# plt.axis('tight')
plt.margins(0.05, 0.0)
# SET IMAGE SIZE - USEFUL FOR OUTSIDE LEGEND
# plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.14)
# plt.tight_layout()
plt.ylim(ymin = 0.1)
plt.savefig('result.pdf')
plt.show()
