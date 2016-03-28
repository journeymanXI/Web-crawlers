'''
Created on July 3, 2014
Modified on July 15, 2014

@author: ashley
'''

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

 
""" class for defining a Queue """
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
 

""" Initializing global data sets """

filepath = "D:\\ahm_webcrawler\\"
#filepath = "D:\\Web Scraper\\"
initial_file = filepath + "Fortune1000\\Links\\Fortune1000_links - 1000.csv"
    
keywords_list = filepath + "Keywords List.xlsx"
bannedwords_list = filepath + "Banned List.xlsx"
    
output_path = filepath + "Fortune1000\\Output\\temp_output\\"
content_path = filepath + "Fortune1000\\Output\\html_content\\"

nonparsedlinks_file = filepath + "Fortune1000\\Output\\nonparsedlinks.csv"
    
output_file = filepath + "Fortune1000\\Output\\output- 1000.csv"

search_words = []
banned_words = []
visited_links = []

weblink_queue = Queue()
homepage_count = 0
next_level_count = 0

level = 0 # starting level - always keep as 0
max_level = 2

initial_matrix = []


""" Function to find out the domain (.com, .org, .gov) and the main url """
def shorten_link(webpage):
    if webpage.find("www") >= 0:
        dot_one = webpage.find(".")
    else :
        dot_one = -1
    dot_two =  webpage[dot_one+1:].find(".")
    backslash = webpage[(dot_one + 1 + dot_two):].find("/")
    if backslash >= 0 :
        domain_name = webpage[dot_one + 1: (dot_one + 1 + dot_two)]
        reduced_webpage = webpage[: (dot_one + 1 + dot_two + backslash)]
    else :
        domain_name = webpage[dot_one + 1:(dot_one + 1 + dot_two)]
        reduced_webpage = webpage

    return domain_name, reduced_webpage


""" Function to access a link, parse it for specific types of url's and give out a dictionary output """
def link_scraper(url,level):
    
    print" Level", level, " %d Threads running" % (threading.activeCount())
    
    global next_level_count
    
    # the fetch_url function fetches the webpage and the html content from the URL given
    webpage, html_content = fetch_url(url)
    
    if webpage != "" :
        # parses the URL for link name and domain name - to be used later
        domain_name, reduced_webpage = shorten_link(webpage)
        
        # search for keywords in a webpage and create a temporary file if it exists, consisting of title, webpage and word freq
        matrix = []
        for word in search_words :
            
            word_finder = re.findall(r'\b'+word+r'\b', html_content)
            
            # checks if word exists in html content; if not, pass
            if len(word_finder) == 0: 
                pass
            else:            
                # create random string as the content key - used as a file name, and to identify each url
                temp_contentkey = "tmp_" + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)) 
                suffix = datetime.datetime.now().strftime("%H%M%S")
                contentkey = ("_".join([temp_contentkey, suffix]))
                
                # Create a tree structure from the html content (currently stored as a string
                # Parse the tree structure to find key words - this is different from the earlier word search
                # In this search, we check if the key word is actually displayed in the web page text
                tree = html.fromstring(html_content)
                content_part = []
                try :
                    for e in tree.xpath("//p"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//a"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//b"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//h1"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//h2"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//h3"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//h4"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//h5"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                    for e in tree.xpath("//li"):
                        if len(re.findall(r'\b'+word+r'\b', e.text_content())) > 0 :
                            content_part.append(e.text_content())
                except :
                    pass

                # Extract the title of the webpage
                try :
                    t = lxml.html.parse(webpage)
                    title = t.find(".//title").text
                    title = title.encode('utf8')
                except :
                    title = ""   
                
                # If the keyword is not displayed as text on the webpage, the URL is not saved
                #row = [webpage, title, reduced_webpage,contentkey]
                if content_part != [] :
                    row = [webpage, title, reduced_webpage]
                    row.append(word)
                    row.append(len(word_finder))
                    row.append(content_part)
                    matrix.append(row)
    
        # if the matrix has content in it, save to a temporary file
        # two files are saved, one containing keywords frequency, and the other containing html content
        # the two files are linked by the content_key - specifited in the third column in the final output file
        # as well as being the name of the html_content file
        if matrix != [] :
            # saving URL and keyword file
            basename = output_path + contentkey + ".csv" # e.g. 'domainname_120508_171442.csv'
            with open(basename, 'wb') as output: 
                writer = csv.writer(output, delimiter=',')
                writer.writerows(matrix)
            # saving content txt file
            #contentname = content_path + contentkey + ".txt" # e.g. 'domainname_120508_171442.txt'
            #with open(contentname, "w") as text_file:
            #    text_file.write(html_content)
            #print threading.current_thread().name, "Saved."
            
    
        # if webpage is a homepage or keywords have been found, add links to queue  
        if level < max_level: #or keyword_found is True:  
            current_page_links = []
    
            # parsing the URL
            for line in re.findall('''href=["'](.[^"']+)["']''', html_content, re.I):
                # isolate web links using http or https, and check if the link is not a duplicate on the current web page
                if ("http://" in line or "https://" in line or line.startswith('/www') or line.startswith('//www')) and (domain_name in line) and (line not in current_page_links) :
                    # find out domain name of the link, and check if it is the same as the domain name of the home page
                    line_domain_name, line_reduced_webpage = shorten_link(line)
                    if domain_name in line_domain_name :
                        current_page_links.append(line)
                        if (line not in visited_links):
                            # check to see if websites are relevant - remove google searches, wiki articles etc - see banned word list
                            banned_word_check = 0
                            for banned_word in banned_words :
                                if (line.lower()).find(banned_word)>=0 :
                                    banned_word_check = 1
                            # add links to the queue
                            if banned_word_check == 0:
                                weblink_queue.push(line)
                                next_level_count += 1
                
                # checking if the link is a bullet point - in this case, the href won't start with a http
                elif line[0] == "/":
                    templine = reduced_webpage + line
                    if (templine not in visited_links) and (templine not in current_page_links):
                        current_page_links.append(line)
                        # check to see if websites are relevant - remove google searches, wiki articles etc - see banned word list
                        banned_word_check = 0
                        for banned_word in banned_words :
                            if (templine.lower()).find(banned_word)>=0 :
                                banned_word_check = 1
                        # add links to the queue
                        if banned_word_check == 0:
                            weblink_queue.push(templine)
                            next_level_count += 1
    
        else: # if webpage is not a homepage, just skip this step
            pass
    else : # if webpage is blank (i.e. the URL was not valid) just skip this step
        pass


""" Function to access a link, parse it for specific types of url's and give out a dictionary output """
def link_grabber(url):
 
    print"%d Threads running" % (threading.activeCount())
    
    # the fetch_url function fetches the webpage and the html content from the URL given
    webpage, html_content = fetch_url(url)
    
    if webpage != "" :
        if ('page=' in webpage) :
            for line in re.findall('''href=["'](.[^"']+)["']''', html_content, re.I):
                # checking if the link is a bullet point - in this case, the href won't start with a http
                if ("/company/view/" in line) :
                    link = "https://connect.data.com" + line
                    weblink_queue.push(link)
                    print "Link added : ", link
        
        matches = re.findall('seo-company-data', html_content)
        if len(matches) == 0: 
            pass
        else :
            # parsing the URL
            for line in re.findall('''class="seo-company-data"><a href=["'](.[^"']+)["']''', html_content, re.I):
                # checking if the link is a bullet point - in this case, the href won't start with a http
                if ("http://" in line or "https://" in line):
                    initial_matrix.append([line])
                    print "Link found : ", line

    else : # if webpage is blank (i.e. the URL was not valid) just skip this step
        pass


""" Function to move the output of a 2-d matrix to a csv file """
def move_to_file(matrix, file):
    with open(file, 'wb') as output:
        writer = csv.writer(output, delimiter=',')
        writer.writerows(matrix)


""" Function to scrape the base website and obtain all links within it - these links will be stored in a file and used later """    
def obtain_all_links(initial_file) :
    
    for i in range(11):
        link = 'https://connect.data.com/directory/company/fortune/1000?page=' + str(i+1)
        weblink_queue.push(link)
 
    # Run while website_queue has links - the links get added to the queue when we visit each starting page
    
    # Main page level
    # Run while the queue has links to access - once the queue is empty, the program exits
    while not weblink_queue.isEmpty():
        webpage = ""
        # make a set of 1000 links, to access simultaneously - change this number depending on the RAM on your system
        url_list = []
        for i in range(100) :
            if not weblink_queue.isEmpty() :
                link = weblink_queue.pop()
                url_list.append(link)
            else :
                break
            
        # create individual threads for each link - link access and parsing is done simultaneously 
        workers = []
        for url in url_list :
            worker_thread = threading.Thread(target=link_grabber, args=(url,))
            worker_thread.start()
            workers.append(worker_thread)
        
        # waits for all 1000 threads to complete before joining (i.e. destroying them)
        # after all threads are destroyed, we move back to the top of the while loop
        for worker in workers:
            worker.join()
            
    move_to_file(initial_matrix, initial_file)


""" Function to read contents (links) in the url file, and input them into a queue """
def urlfile_reader(file, rerun):
    rownum = 0
    global level
    global homepage_count
    global next_page_count
    
    if rerun == False :
        column = 1 # here, the program reads from the base input file
    elif rerun == True :
        column = 0 # here, the program reads from the temporary file, since it was paused mid way
    
    with open(file,'rb') as readfile:
        reader = csv.reader(readfile)
        for row in reader:
            if rownum == 0 :
                if rerun == True :
                    homepage_count = row[0]
                    next_page_count = row[1]
                    level = row[2]
                rownum = rownum + 1
            else :
                if "statelocalgov" not in row[column] :
                    weblink_queue.push(row[column]) 
                    if rerun == False :
                        homepage_count = homepage_count + 1

""" Function to fetch the URL and HTML CONTENT from a given link """
def fetch_url(url):
    if random.randrange(10) <= 1 :
        print "Current Running time : ", datetime.datetime.now() - start
        print "Opening : ", url
    try :
        webpage = urllib2.urlopen(url, timeout=1).geturl()
        html_content = urllib.urlopen(webpage, timeout=1).read()
        return webpage, html_content
    except :
        try :
            webpage = urllib2.urlopen(url,timeout=2).geturl()
            html_content = urllib2.urlopen(webpage,timeout=2).read()
            return webpage, html_content
        except :
            return "", ""    
    
        
""" Returns a set of URLs from the queue, in batches of size :size """
def multiple_url_fetcher(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


""" Final save that is done upon exiting the program """
def file_save():
    
    global level
    global homepage_count
    global next_level_count
    
    # save output file
    dir = output_path + '*.csv'    
    with open(output_file, 'wb') as outfile:
        for filename in glob.glob(dir):
            with open(filename) as readfile:
                shutil.copyfileobj(readfile, outfile)              
    print "Created final file : ", datetime.datetime.now()
    
    # Removing duplicates in final file
    in_file = csv.reader(open(output_file, "rb"))
    newrows = []
    for row in in_file:
        if row not in newrows:
            newrows.append(row)
    out_file = csv.writer(open(output_file, "wb"))
    out_file.writerows(newrows)
    print "Removed duplicates from final file : ", datetime.datetime.now()
    
    # save links
    with open(nonparsedlinks_file, 'wb') as output: 
        writer = csv.writer(output, delimiter=',')
        row = [homepage_count, next_level_count, level] # create a row containing the homepage count, next level count and current level
        writer.writerow([row]) # This information is stored so that it can be used when the code is resumed
        while not weblink_queue.isEmpty():
            row = weblink_queue.pop()
            writer.writerow([row])


def main(rerun):
    
    # change to True when you want to restart a run -  this should be done only after an unexpected termination in the previous run
    # the code picks up at the link where the previous run was stopped at
    # DO NOT set this to True if you are running for the first time
    #rerun = False 
    
    # needs to be initialized in every function that accesses these global variables 
    global level
    global homepage_count
    global next_level_count
    
    print "Importing Key word list and Banned word list..."
    
    # Key words to be searched in each web page - modify keywords_list to change the set of words
    workbook = xlrd.open_workbook(keywords_list)
    worksheet = workbook.sheet_by_name('Sheet1')

    rowcount = 1 # starting from 1 to exclude the header
    while rowcount < worksheet.nrows:
        row = worksheet.row(rowcount)
        search_words.append(str(worksheet.cell_value(rowcount, 0).encode('utf8')))
        rowcount += 1
    
    
    # Words to be banned on each web page - modify bannedwords_lists to edit which words should be flagged down
    workbook = xlrd.open_workbook(bannedwords_list)
    worksheet = workbook.sheet_by_name('Sheet1')

    rowcount = 1 # starting from 1 to exclude the header
    while rowcount < worksheet.nrows:
        row = worksheet.row(rowcount)
        banned_words.append(str(worksheet.cell_value(rowcount, 0).encode('utf8')))
        rowcount += 1
    
    print "Current Running time : ", datetime.datetime.now() - start
    
    
    
    # Run this initially to obtain all the links in the base website - skip if this is already done and directly import to data file
    #obtain_all_links(initial_file)
    
    
    
    print "Reading file..."
    
    # checks if the code is a re-run i.e. if there was an unexpected termination in the previous run
    # and we need to restart from that point
    if rerun == False :
        urlfile_reader(initial_file, rerun) # change according to which file to use
    if rerun == True :
        urlfile_reader(nonparsedlinks_file, rerun) # change according to which file to use
    # creates a list of home pages i.e. base links (and not the parsed URLs)

    
    # Run while the queue has links to access - once the queue is empty, the program exits
    link_count = 0
    while not weblink_queue.isEmpty():
        webpage = ""
        # make a set of 500 links, to access simultaneously - change this number depending on the RAM on your system
        url_list = []
        for i in range(200) :
            if not weblink_queue.isEmpty() :
                link = weblink_queue.pop()
                url_list.append(link)
                link_count = link_count + 1
            else :
                break
        
        if link_count > homepage_count :
            link_count = link_count - homepage_count
            homepage_count = next_level_count
            next_level_count = 0
            level = level + 1
            
        # create individual threads for each link - link access and parsing is done simultaneously 
        workers = []
        for url in url_list :
            worker_thread = threading.Thread(target=link_scraper, args=(url,level,))
            worker_thread.start()
            workers.append(worker_thread)
        
        print "Link Count : ", link_count , "Out of ", homepage_count, "on level ", level
        
        # waits for all 1000 threads to complete before joining (i.e. destroying them)
        # after all threads are destroyed, we move back to the top of the while loop
        for worker in workers:
            worker.join()


    print "Current Running time : ", datetime.datetime.now() - start
    # at the end of the code, save all the files in the temp folder to the final output file
    # the file_save function does this for you
    file_save()


if __name__ == '__main__':
    start = datetime.datetime.now()
    print "Starting time : ", start
    
    # arguments passed here - either True or False
    # False during First run ; True after a Pause
    
    # performs file save on exit
    atexit.register(file_save)
    
    # main code
    #main(rerun=False)
    main(rerun=sys.argv[1])

    