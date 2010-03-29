#!/usr/bin/env pythonw
# encoding: utf-8
# requires BeautifulSoup
# based on Flowing Data blog post: 
# http://flowingdata.com/2009/11/12/how-to-make-a-us-county-thematic-map-using-free-tools/
#
# Usage:
# $ python colorize_svg.py > twitter_users.svg
 
import csv
from BeautifulSoup import BeautifulSoup, Tag
from math import log
import time 
# import pylab

# read in twitter user counts & county populations...
usercount = {}
reader = csv.reader(open('county_counts.txt'), delimiter="\t")
for row in reader:
  try:
    fips = row[0]
    usercount[fips] = int(row[1])
  except:
    pass

max_users = max(usercount.values())
min_users = min(usercount.values())

# # Read in linkedin membership counts
# membercount = {}
# reader = csv.reader(open('linkedin_us_heatmap.txt'), delimiter="\t")
# for row in reader:
#     try:
#         full_fips = row[0]
#         count = int(row[1])
#         membercount[full_fips] = count
#     except:
#         pass
# 
# # Read in unemployment rates
# # LAUS_CODE       STATE_FIPS      COUNTY_FIPS     COUNTY_NAME     PERIOD  LABOR_FORCE     EMPLOYED        UNEMPLOYED      UNEMPLOYMENT_RATE
# 
# # $ grep 'Santa Clara' unemployment_statistics.txt 
# # CN060850  06  085 Santa Clara County, CA  Nov-08  891,839       829,011       62,828      7.0     
# # CN060850  06  085 Santa Clara County, CA  Dec-08  892,591       823,967       68,624      7.7     
# # CN060850  06  085 Santa Clara County, CA  Jan-09  895,079       811,537       83,542      9.3     
# # CN060850  06  085 Santa Clara County, CA  Feb-09  897,328       807,363       89,965      10.0     
# # CN060850  06  085 Santa Clara County, CA  Mar-09  898,775       800,526       98,249      10.9     
# # CN060850  06  085 Santa Clara County, CA  Apr-09  895,472       797,743       97,729      10.9     
# # CN060850  06  085 Santa Clara County, CA  May-09  887,305       787,206       100,099       11.3     
# # CN060850  06  085 Santa Clara County, CA  Jun-09  893,221       787,661       105,560       11.8     
# # CN060850  06  085 Santa Clara County, CA  Jul-09  900,383       794,372       106,011       11.8     
# # CN060850  06  085 Santa Clara County, CA  Aug-09  898,207       789,980       108,227       12.0     
# # CN060850  06  085 Santa Clara County, CA  Sep-09  887,762       782,935       104,827       11.8     
# # CN060850  06  085 Santa Clara County, CA  Oct-09  887,459       781,793       105,666       11.9     
# # CN060850  06  085 Santa Clara County, CA  Nov-09  882,492       778,649       103,843       11.8     
# # CN060850  06  085 Santa Clara County, CA  Dec-09  875,389       777,048       98,341      11.2
# 
# labor_force = {}
# employed = {}
# unemployed = {}
# unemployment_rate = {}
# 
# reader = csv.reader(open('unemployment_statistics_dec09.txt'), delimiter="\t")
# for row in reader:
#     try:
#         full_fips = row[1] + row[2]
#         labor_force[full_fips] = int(row[5].replace(',',''))
#         employed[full_fips] = int(row[6].replace(',',''))
#         unemployed[full_fips] = int(row[7].replace(',',''))
#         rate = float( row[8].strip() )
#         unemployment_rate[full_fips] = rate
#     except:
#         pass


# we want to insert hover tooltips into the SVG for use in data QA...

# 
# <path id="06037"
#  style="font-size:12px;fill:#d0d0d0;fill-rule:nonzero;stroke:#000000;stroke-opacity:1;stroke-width:0.1;stroke-miterlimit:4;stroke-dasharray:none;stroke-linecap:butt;marker-start:none;stroke-linejoin:bevel"
#    d="M 54.34898,214.1 L 53.54698,214.069 L 53.03798,213.442 L 52.96998,213.334 L 52.72198,212.545 L 52.49698,211.135 L 52.78098,211.23 L 54.25898,213.807 L 54.38098,214.069 L 54.34898,214.1M 53.80398,205.321 L 53.75898,205.288 L 53.74998,205.195 L 53.75898,205.177 L 53.80798,205.19 L 54.35898,205.375 L 54.97598,205.871 L 55.47598,206.272 L 55.85498,206.592 L 56.34198,207.601 L 56.38198,207.705 L 56.40898,207.921 L 56.35998,208.038 L 56.21098,208.125 L 56.20198,208.129 L 56.11598,208.129 L 54.81798,207.516 L 54.73198,207.439 L 53.80398,205.321M 67.45198,190.511 L 66.76598,193.485 L 66.44698,195.203 L 66.20698,196.663 L 66.11198,197.294 L 65.41898,198.794 L 64.81998,199.795 L 64.58098,200.192 L 63.83198,200.556 L 63.56198,200.886 L 63.54398,200.976 L 63.59298,201.179 L 63.67898,201.301 L 62.26398,200.976 L 62.14198,200.949 L 61.65998,200.918 L 60.52398,201.895 L 59.94698,202.621 L 59.67198,203.107 L 58.86998,203.216 L 58.02698,203.198 L 57.63898,203.045 L 57.44598,202.936 L 56.69298,202.45 L 56.55798,202.338 L 56.47698,201.883 L 56.55298,201.802 L 56.79698,201.621 L 56.92298,201.594 L 56.98598,201.464 L 57.05298,201.139 L 56.97298,200.544 L 56.92298,200.255 L 56.89598,200.093 L 56.83798,199.759 L 56.73398,199.353 L 56.58898,198.962 L 56.57598,198.934 L 56.55798,198.893 L 56.55298,198.885 L 56.44098,198.664 L 56.41798,198.633 L 56.33298,198.511 L 56.30498,198.471 L 56.27398,198.443 L 56.09298,198.272 L 55.83198,198.169 L 54.79998,197.952 L 53.98898,197.844 L 53.56098,197.88 L 51.96498,197.172 L 52.09998,196.811 L 53.12798,196.365 L 53.95698,196.018 L 55.20098,196.325 L 55.83198,195.319 L 55.89998,194.847 L 54.92198,187.946 L 54.87698,187.572 L 67.45198,190.511"
#    inkscape:label="Los Angeles, CA" />
# <text id="thepopup" x="54.34898" y="214.1" font-size="10" fill="black" visibility="hidden">Los Angeles, CA (06037)
#   <set attributeName="visibility" from="hidden" to="visible" begin="06037.mouseover" end="06037.mouseout"/>
# </text>


 
# Load the SVG map
svg = open('counties.svg', 'r').read()
 
# Load into Beautiful Soup
soup = BeautifulSoup(svg, selfClosingTags=['defs','sodipodi:namedview'])
 
# Find counties
paths = soup.findAll('path')
 
# Map colors
# colors = [ "#9ECAE1", "#6BAED6", "#4292C6", "#2171B5", "#08519C", "#08306B"]

# too whitewashed...
# colors = ["#F7FBFF", "#DEEBF7", "#C6DBEF", "#9ECAE1", "#6BAED6", "#4292C6", "#2171B5", "#08519C", "#08306B"]

colors = ["#DEEBF7", "#C6DBEF", "#9ECAE1", "#6BAED6", "#4292C6", "#2171B5", "#08519C", "#08306B"]

# County style
path_style = 'font-size:12px;fill-rule:nonzero;stroke:#FFFFFF;stroke-opacity:1;stroke-width:0.1;stroke-miterlimit:4;stroke-dasharray:none;stroke-linecap:butt;marker-start:none;stroke-linejoin:bevel;fill:'

max_color = 0

fipsvals = []
counts = []
labor_cnts = []
 
# Color the counties based on values
# for p in paths:
#   if p['id'] not in ["State_Lines", "separator"]:
#     try:
#       count = membercount[p['id']]
#       labor = labor_force[p['id']]
#     except:
#       continue 
#     # color_class = int((float(len(colors)-1) * float(count - min_value)) / float(max_value - min_value))
#     color_class = int(log(count + 1.0))
#     max_color = max(max_color, color_class)
#     fipsvals.append(p['id'])
#     counts.append(count)
#     labor_cnts.append(labor)
#     # color = colors[color_class]
#     # p['style'] = path_style + color
        
# max_value = int(log(max(counts)+1.0))
# min_value = int(log(min(counts)+1.0))
# 
# memb_percent = 10*array(counts)/(array(labor_cnts)+1.0)

# we will append this hover tooltip after each county path
hover_text = '''<text id="popup-%s" x="%s" y="%s" font-size="10" fill="black" visibility="hidden">%s (%s)<set attributeName="visibility" from="hidden" to="visible" begin="%s.mouseover" end="%s.mouseout"/></text>'''


for p in paths:
  if p['id'] not in ["State_Lines", "separator"]:
    try:
      count = usercount[p['id']]
    except:
      # print "Missing", p["id"] 
      count = 0
    x, y = (p['d'].split()[1]).split(',')
    # print p['id'], p['inkscape:label'], x, y
    # insert a new text tag for the county hover tooltip...
    p.parent.insert(0, Tag(soup, 'text', [("id", 'popup-'+p['id'])]))
    hover = soup.find("text", { "id" :  'popup-'+p['id'] })
    hover.insert(1, "%s (%s)" % (p['inkscape:label'], str(count)))
    # add attributes to that text tag...
    hover['x'] = 250
    hover['y'] = 20
    hover['font-size'] = "20"
    hover['fill'] = "black"
    hover['visibility'] = "hidden"
    hover.insert(0, Tag(soup, 'set', [("begin", p['id']+'.mouseover')]))
    set_tag = soup.find("set", { "begin" :  p['id']+'.mouseover' })
    set_tag['attributeName'] = "visibility" 
    set_tag['from'] = "hidden" 
    set_tag['to'] = "visible" 
    set_tag['end'] = p['id']+'.mouseout'
    # pass
    color_class = min(int(log(count +1)), len(colors)-1)     
    # color_class = min(int(10*count/(labor+1.0)), len(colors)-1)   
    # color_class = min(int(log(count + 1.0))-min_value, len(colors)-1)    
    # color_class = int((float(len(colors)-1) * float(count - min_value)) / float(max_value - min_value))
    color = colors[color_class]
    p['style'] = path_style + color    


# pylab.hist(memb_percent, 100) 
# pylab.show()

print soup.prettify()


