#!/usr/bin/env pythonw
# encoding: utf-8
# requires BeautifulSoup
# based on Flowing Data blog post: 
# http://flowingdata.com/2009/11/12/how-to-make-a-us-county-thematic-map-using-free-tools/
#
# Usage:
# $ ./colorize_svg.py -f county_counts.txt > twitter_users.svg
#
# Expects a file of county_counts containing two columns:
# fipscode, count (integers)
#
# 51770   1
# 13089   1
# 54011   1
# 54039   3
# 12117   2
#
# Also require a baseline svg file in the same directory called counties.svg
#
 
import csv
from BeautifulSoup import BeautifulSoup, Tag
from math import log
import time 
import getopt
import os, sys

class Usage(Exception):
  def __init__(self, msg):
    self.msg = msg

def load_intensities(filename):
  intensities = {}
  reader = csv.reader(open(filename), delimiter="\t")
  for row in reader:
    try:
      fips = row[0]
      intensities[fips] = int(row[1])
    except:
      pass
  return intensities
  
def generate_heatmap(intensities):
  # Load the SVG map
  svg = open('counties.svg', 'r').read()
  # Load into Beautiful Soup
  soup = BeautifulSoup(svg, selfClosingTags=['defs','sodipodi:namedview'])
  # Find counties
  paths = soup.findAll('path')
  colors = ["#DEEBF7", "#C6DBEF", "#9ECAE1", "#6BAED6", "#4292C6", "#2171B5", "#08519C", "#08306B"]
  min_value = min(intensities.values())
  max_value = max(intensities.values())
  scalefactor = (len(colors)-1)/(log(max_value +1)-log(min_value +1))
  # County style
  path_style = 'font-size:12px;fill-rule:nonzero;stroke:#FFFFFF;stroke-opacity:1;stroke-width:0.1;stroke-miterlimit:4;stroke-dasharray:none;stroke-linecap:butt;marker-start:none;stroke-linejoin:bevel;fill:'
  # we will append this hover tooltip after each county path
  hover_text = '''<text id="popup-%s" x="%s" y="%s" font-size="10" fill="black" visibility="hidden">%s (%s)<set attributeName="visibility" from="hidden" to="visible" begin="%s.mouseover" end="%s.mouseout"/></text>'''
  for p in paths:
    if p['id'] not in ["State_Lines", "separator"]:
      try:
        count = intensities[p['id']]
      except: 
        count = 0
      x, y = (p['d'].split()[1]).split(',')
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
      color_class = min(int(scalefactor*log(count +1)), len(colors)-1)  
      # color_class = int((float(len(colors)-1) * float(count - min_value)) / float(max_value - min_value))
      # if count > 0:
      #   print color_class
      color = colors[color_class]
      p['style'] = path_style + color    
  print soup.prettify()

def main(argv=None):
  if argv is None:
    argv = sys.argv
  try:
    try:
      opts, args = getopt.getopt(argv[1:], "hf:v", ["help", "filename="])
    except getopt.error, msg:
      raise Usage(msg)

    # option processing
    for option, value in opts:
      if option == "-v":
        verbose = True
      if option in ("-h", "--help"):
        raise Usage(help_message)
      if option in ("-f", "--filename"):
        filename = value
  
    # main processing
    intensities = load_intensities(filename)
    generate_heatmap(intensities)    

  except Usage, err:
    print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
    print >> sys.stderr, "\t for help use --help"
    return 2


if __name__ == "__main__":
  sys.exit(main())


