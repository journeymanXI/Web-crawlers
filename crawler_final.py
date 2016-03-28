'''
Created on July 18, 2014
Last modified on July 28, 2014

@author: Jim D'Souza

Description : Web Crawler that searches for specific keywords in the websites of a list of companies.
The list needs to be given as input.
The crawler finds the company home pages of these companies using Bing (Google has more stringest crawler restrictions, Yahoo results are not relevant enough)
It then parses through the company website, including all relevant links away from the site, to find the keywords
'''

# Import relevant packages
import random
import atexit

import httplib
import time
import threading

import glob
import shutil

import itertools
import sys
import time
import datetime

import string
import xlrd
import csv
import urllib
import urllib2
import re
import lxml.html

from lxml import html

 
### Class for defining a Queue ###
### The Queue is needed to store the links, and push them out in the order they came in ###
### This is relevant to denote which level the crawler has reached ###
### A level will be described later ###
class Queue:
    def __init__(self):
        self.head = 0
        self.tail = 0
        self.n = 0
        self.items = []
     
    def push(self, item):
        # this will create a lot of wasted space. Use something like
        # linked list
        self.items.append(item)
        self.tail += 1
        self.n += 1
     
    def pop(self):
        e = self.items[self.head]
        self.head += 1
        self.n -= 1
        return e
    
    # method to find the number of elements in the queue     
    def size(self):
        return self.n
    
    # method to check whether the queue is empty or not
    def isEmpty(self):
        if self.n == 0:
            return True
        else :
            return False
     
    # method to display all the elements in the queue
    def elements(self):
        return [self.l[i] for i in xrange(self.head, self.tail)]
 

### Initializing global data sets ###

#filepath = "D:\\ahm_webcrawler\\LinkedIn\\"
filepath = "D:\\Web Scraper\\LinkedIn\\"
initial_file = filepath + "Links\\links - novartis.csv"
    
keywords_list = filepath + "Keywords List.xlsx"
bannedwords_list = filepath + "Banned List.xlsx"
company_list_file = filepath + "Company List.xlsx"
    
output_path = filepath + "Output\\temp_output\\"
content_path = filepath + "Output\\html_content\\"

nonparsedlinks_file = filepath + "Output\\nonparsedlinks.csv"
    
output_file = filepath + "Output\\output.csv"

search_words = []
banned_words = []
company_list = []
visited_links = []

weblink_queue = Queue()
homepage_count = 0
next_level_count = 0

### Starting level - always keep as 0 ###
level = 0 
max_level = 2

initial_matrix = []


### Function to access a link, parse it for specific types of url's and give out a dictionary output ###
def link_scraper(url,level):
    
	### Prints the level at which the scraper is working ###
	### A level is the number of links from the base link that the scraper has followed ###
	### Eg : The first set of links added to the queue is level 0. Any links present within these are level 1. Links within level 1 are level 2, and so on. ###
    print" Level", level, " %d Threads running" % (threading.activeCount())
    
	### Keeps track of the current level ###
    global next_level_count
    
    ### The fetch_url function fetches the webpage and the html content from the URL given ###
    webpage, html_content = fetch_url(url)    
    
    if webpage != "" :        
        ### Search for keywords in a webpage and create a temporary file if it exists, consisting of title, webpage and word freq ###
        matrix = []

        ### Extract the title of the webpage
        try :
            t = lxml.html.parse(webpage) 
            name = t.find(".//title").text
            name = name.encode('utf8')
        except :
            name = ""
            
        try :     
            t = lxml.html.parse(webpage)
            title = ""
            if title == "" :   
                title = t.find(".//p").text
                if ' at ' not in title and ' at ' not in title:
                    title = ""
                title = title.encode('utf8')
            
                if title == "" :
                    title = t.find(".//dd").text
                    if ' at ' not in title and ' at ' not in title:
                        title = ""
                    title = title.encode('utf8')

        except :
            title = ""
            
        try :
            company = ""
            co_pos = title.find(' at ')
            company = title[co_pos+4:]
            print company
        except :
            company = ""
                
        ### If the keyword is not displayed as text on the webpage, the URL is not saved ###
        #row = [webpage, title, reduced_webpage,contentkey]
        title = title.replace("\n","")
        row = [name, title, company]
        matrix.append(row)
    
        ### If the matrix has content in it, save to a temporary file ###
        if name != "" and title != "" :
            print matrix
            ### Saving URL and keyword file ###
            ### Create random string as the content key - used as a file name, and to identify each url ###
            temp_contentkey = "tmp_" + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)) 
            suffix = datetime.datetime.now().strftime("%H%M%S")
            contentkey = ("_".join([temp_contentkey, suffix]))
            basename = output_path + contentkey + ".csv" # e.g. 'domainname_120508_171442.csv'
            move_to_file(matrix, basename)
            
    else : ### If webpage is blank (i.e. the URL was not valid) just skip this step ###
        pass


### Function to access a link, parse it for specific types of url's and give out a dictionary output ###
def link_grabber(url):

    ### The fetch_url() function fetches the webpage and the html content from the URL given ###
    webpage, html_content = fetch_url(url)
    
    print webpage
    print html_content
    
    if webpage != "" :
        for line in re.findall('''href=["'](.[^"']+)["']''', html_content, re.I):
            if 'www.linkedin.com' in line and line not in initial_matrix:
                initial_matrix.append([line])
                print "Link found : ", line

    else : ### If webpage is blank (i.e. the URL was not valid) just skip this step ###
        pass


### Function to move the output of a 2-d matrix to a csv file ###
def move_to_file(matrix, file):
    with open(file, 'wb') as output:
        writer = csv.writer(output, delimiter=',')
        writer.writerows(matrix)


### Function to scrape the base website and obtain all links within it - these links will be stored in a file and used later ###   
def obtain_all_links(company_list, initial_file) :
    
    for company in company_list :
        link = "http://www.bing.com/search?q=%22"+company+"%22+%22research%22+%22United+States%22+-jobs+site%3Alinkedin.com&count=50"
        weblink_queue.push(link)
        link = "http://www.bing.com/search?q=%22"+company+"%22+%22research%22+%22United+States%22+-jobs+site%3Alinkedin.com&count=50&first=51"
        weblink_queue.push(link)
        link = "http://www.bing.com/search?q=%22"+company+"%22+%22research%22+%22United+States%22+-jobs+site%3Alinkedin.com&count=50&first=101"
        weblink_queue.push(link)
        link = "http://www.bing.com/search?q=%22"+company+"%22+%22research%22+%22United+States%22+-jobs+site%3Alinkedin.com&count=50&first=151"
        weblink_queue.push(link)

    ### Run while website_queue has links - the links get added to the queue when we visit each starting page ###
    
    ### Main page level ###
    ### Run while the queue has links to access - once the queue is empty, the crawler exits ###
    while not weblink_queue.isEmpty():
        webpage = ""
        ### Make a set of 1000 links, to access simultaneously - change this number depending on the RAM on your system ###
        url_list = []
        for i in range(10) :
            if not weblink_queue.isEmpty() :
                link = weblink_queue.pop()
                url_list.append(link)
            else :
                break
            
        ### Create individual threads for each link - link access and parsing is done simultaneously ###
        for url in url_list :
            link_grabber(url)
    
	### Save the output to a temp file ###
    move_to_file(initial_matrix, initial_file)


### Function to read contents (links) in the url file, and input them into a queue ###
def urlfile_reader(file, rerun):
	### Global vars needed to keep track of the level, and the number of URL's on that level ###
    rownum = 0
    global level
    global homepage_count
    global next_page_count
    
    column = 0
    
	### Reading all URL's at that level and pushing into the queue ###
	### Important to keep track of the number of URL's at this level ###
    with open(file,'rb') as readfile:
        reader = csv.reader(readfile)
        for row in reader:
            if rownum == 0 and rerun == True :
                homepage_count = row[0]
                next_page_count = row[1]
                level = row[2]
                rownum = rownum + 1
            else :
                weblink_queue.push(row[column]) 
                if rerun == False :
                    homepage_count = homepage_count + 1

					
### Function to fetch the URL and HTML CONTENT from a given link ###
def fetch_url(url):
    #if random.randrange(10) <= 1 :
    webpage = url
    print "Current Running time : ", datetime.datetime.now() - start
    print "Opening : ", url
    try :
        #webpage = urllib2.urlopen(url, timeout=15).geturl()
        html_content = urllib2.urlopen(url, timeout=15).read()
        return webpage, html_content
    except :
        try :
            #webpage = urllib2.urlopen(url,timeout=30).geturl()
            html_content = urllib2.urlopen(url, timeout=15).read()
            return webpage, html_content
        except :
            return "", ""  
    
        
### Returns a set of URLs from the queue, in batches of size :size ###
def multiple_url_fetcher(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


### Final save that is done upon exiting the program ###
def file_save():
    
	### Global vars to keep track of the level ###
    global level
    global homepage_count
    global next_level_count
    
    ### Save output file ###
    dir = output_path + '*.csv'    
    with open(output_file, 'wb') as outfile:
        for filename in glob.glob(dir):
            with open(filename) as readfile:
                shutil.copyfileobj(readfile, outfile)              
    print "Created final file : ", datetime.datetime.now()
    
    ### Removing duplicates in final file ###
	### A lot of links lead to the same secondary link ###
	### This would increase the overhead greatly, since we would be repeatedly accessing the same links, and adding the same next level links to the queue ###
	### There is an overhead here. Try to find ways to reduce this ###
    in_file = csv.reader(open(output_file, "rb"))
    newrows = []
    for row in in_file:
        if row not in newrows:
            newrows.append(row)
    out_file = csv.writer(open(output_file, "wb"))
    out_file.writerows(newrows)
    print "Removed duplicates from final file : ", datetime.datetime.now()
    
    ### Save links ###
    with open(nonparsedlinks_file, 'wb') as output: 
        writer = csv.writer(output, delimiter=',')
        row = [homepage_count, next_level_count, level]
        while not weblink_queue.isEmpty():
            row = weblink_queue.pop()
            writer.writerow([row])


def main(rerun):
    
    ### Change to True when you want to restart a run -  this should be done only after an unexpected termination in the previous run ###
    ### the code picks up at the link where the previous run was stopped at ###
    ### DO NOT set this to True if you are running for the first time ###
    #rerun = False 
    
	### Global vars to keep track of the levels and the count of URL's at each level ###
    ### Needs to be initialized in every function that accesses these global variables  ###
    global level
    global homepage_count
    global next_level_count
    
    print "Importing Key word list and Banned word list..."
    
    ### Key words to be searched in each web page - modify keywords_list to change the set of words ###
    workbook = xlrd.open_workbook(keywords_list)
    worksheet = workbook.sheet_by_name('Sheet1')

    rowcount = 1 # starting from 1 to exclude the header
    while rowcount < worksheet.nrows:
        row = worksheet.row(rowcount)
        search_words.append(str(worksheet.cell_value(rowcount, 0).encode('utf8')))
        rowcount += 1
    
    ### Words to be banned on each web page - modify bannedwords_lists to edit which words should be flagged down ###
    workbook = xlrd.open_workbook(bannedwords_list)
    worksheet = workbook.sheet_by_name('Sheet1')
    rowcount = 1 # starting from 1 to exclude the header
    while rowcount < worksheet.nrows:
        row = worksheet.row(rowcount)
        banned_words.append(str(worksheet.cell_value(rowcount, 0).encode('utf8')))
        rowcount += 1
    
    
    print "Importing Company list..."
    
    ### Key words to be searched in each web page - modify keywords_list to change the set of words ###
    workbook = xlrd.open_workbook(company_list_file)
    worksheet = workbook.sheet_by_name('Sheet1')

    rowcount = 1 # starting from 1 to exclude the header
    while rowcount < worksheet.nrows:
        row = worksheet.row(rowcount)
        company_list.append(str(worksheet.cell_value(rowcount, 0).encode('utf8')))
        rowcount += 1

    
    print "Current Running time : ", datetime.datetime.now() - start
    
    ### Run this initially to obtain all the links in the base website - skip if this is already done and directly import to data file ###
    #obtain_all_links(company_list, initial_file)
    
       
    print "Reading file..."
    
    ### Checks if the code is a re-run i.e. if there was an unexpected termination in the previous run ###
    ### and if we need to restart from that point ###
    ### Creates a list of home pages i.e. base links (and not the parsed URLs) ###
    urlfile_reader(initial_file, rerun) 
    
    ### Run while the queue has links to access - once the queue is empty, the program exits ###
    link_count = 0
    while not weblink_queue.isEmpty():
        webpage = ""
        ### Make a set of 1000 links, to access simultaneously - change this number depending on the RAM on your system ###
        url_list = []
        for i in range(100) :
            if not weblink_queue.isEmpty() :
                link = weblink_queue.pop()
                url_list.append(link)
                link_count = link_count + 1
            else :
                break
        print url_list
        
        if link_count > homepage_count :
            link_count = link_count - homepage_count
            homepage_count = next_level_count
            next_level_count = 0
            level = level + 1
            
        ### Create individual threads for each link - link access and parsing is done simultaneously ###
        workers = []
        for url in url_list :
            worker_thread = threading.Thread(target=link_scraper, args=(url,level,))
            worker_thread.start()
            workers.append(worker_thread)
        
        print "Link Count : ", link_count , "Out of ", homepage_count, "on level ", level
        
        ### Waits for all 1000 threads to complete before joining (i.e. destroying them) ###
        ### After all threads are destroyed, we move back to the top of the while loop ###
        for worker in workers:
            worker.join()


    print "Current Running time : ", datetime.datetime.now() - start
    ### At the end of the code, save all the files in the temp folder to the final output file ###
    ### The file_save function does this for you ###
    file_save()


if __name__ == '__main__':
    start = datetime.datetime.now()
    print "Starting time : ", start
    
    ### Arguments passed here - either True or False ###
    ### False during First run ; True after a Pause ###
    
    ### Performs file save on exit
    #atexit.register(file_save)
    
    # main code
    main(rerun=False)

    