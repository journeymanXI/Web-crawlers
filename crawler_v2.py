'''
Created on Jun 16, 2014
Modified on July 2, 2014

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

state_file = "D:\\Web Scraper\\Links\\state_file.csv"
local_file = "D:\\Web Scraper\\Links\\local_file.csv"
allstates_file = "D:\\Web Scraper\\Links\\allstates_file.csv"
national_file = "D:\\Web Scraper\\Links\\national_file.csv"
    
keywords_list = "D:\Web Scraper\Keywords List.xlsx"
bannedwords_list = "D:\Web Scraper\Banned List.xlsx"
    
state_path = "D:\\Web Scraper\\Output\\temp_state\\"
content_path = "D:\\Web Scraper\\Output\\html_content\\"

nonparsedlinks_file = "D:\\Web Scraper\\Output\\nonparsedlinks.csv"
    
state_output = "D:\Web Scraper\Output\State_output.csv"
local_output = "D:\Web Scraper\Output\Local_output.csv"
allstates_output = "D:\Web Scraper\Output\Allstates_output.csv"
national_output = "D:\Web Scraper\Output\National_output.csv"

search_words = []
banned_words = []
visited_links = []

weblink_queue = Queue()
weblink_array = []
homepage_array = []


""" Function to find out the domain (.com, .org, .gov) and the main url """
def shorten_link(webpage):
    if webpage.find("www") >= 0:
        dot_one = webpage.find(".")
    else :
        dot_one = -1
    dot_two =  webpage[dot_one+1:].find(".")
    backslash = webpage[(dot_one + 1 + dot_two):].find("/")
    if backslash >= 0 :
        #domain = webpage[(dot_one + 1 + dot_two):(dot_one + 1 + dot_two + backslash)]
        domain_name = webpage[dot_one + 1: (dot_one + 1 + dot_two)]
        reduced_webpage = webpage[: (dot_one + 1 + dot_two + backslash)]
    else :
        #domain = webpage[(dot_one + 1 + dot_two):]
        domain_name = webpage[dot_one + 1:(dot_one + 1 + dot_two)]
        reduced_webpage = webpage[:(dot_one + 1 + dot_two + backslash)]

    return domain_name, reduced_webpage


""" Function to access a link, parse it for specific types of url's and give out a dictionary output """
def link_scraper(url):

    print"%d Threads running" % (threading.activeCount())
    
    # the fetch_url function fetches the webpage and the html content from the URL given
    webpage, html_content = fetch_url(url)
    
    if webpage != "" :
        # parses the URL for link name and domain name - to be used later
        domain_name, reduced_webpage = shorten_link(webpage)
    
        # search for keywords in a webpage and create a temporary file if it exists, consisting of title, webpage and word freq
        matrix = []
        for word in search_words :
            matches = re.findall(word, html_content)
            if len(matches) == 0: 
                pass
            else:            
                # highlight keyword in yellow if found
                #html_content = html_content.replace(word, '<span style="background-color: #FFFF00">'+word+'</span>')
                #website_dictionary[webpage]['content'] = html_content
                temp_contentkey = "tmp_" + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)) 
                suffix = datetime.datetime.now().strftime("%H%M%S")
                contentkey = ("_".join([temp_contentkey, suffix]))
                # extract the title of the webpage
                try :
                    t = lxml.html.parse(webpage)
                    title = t.find(".//title").text
                    title = title.encode('utf8')
                except :
                    title = ""   
            
                row = [webpage, title, reduced_webpage,contentkey]
                row.append(word)
                row.append(len(matches))
                matrix.append(row)
    
        # if the matrix has content in it, save to a temporary file
        # two files are saved, one containing keywords frequency, and the other containing html content
        # the two files are linked by the content_key - specifited in the third column in the final output file
        # as well as being the name of the html_content file
        if matrix != [] :
            # saving URL and keyword file
            basename = state_path + contentkey + ".csv" # e.g. 'domainname_120508_171442.csv'
            with open(basename, 'wb') as output: 
                writer = csv.writer(output, delimiter=',')
                writer.writerows(matrix)
            # saving content txt file
            contentname = content_path + contentkey + ".txt" # e.g. 'domainname_120508_171442.txt'
            with open(contentname, "w") as text_file:
                text_file.write(html_content)
            print threading.current_thread().name, "Saved."
            
    
        # if webpage is a homepage or keywords have been found, add links to queue  
        if webpage in homepage_array : #or keyword_found is True:  
            current_page_links = []
    
            # parsing the URL
            for line in re.findall('''href=["'](.[^"']+)["']''', html_content, re.I):
                # checking if the link is a bullet point - in this case, the href won't start with a http
                if line[0] == "/":
                    templine = reduced_webpage + line
                    if (templine not in visited_links) and (templine not in current_page_links) and (templine not in weblink_array):
                        current_page_links.append(line)
                        # check to see if websites are relevant - remove google searches, wiki articles etc - see banned word list
                        banned_word_check = 0
                        for banned_word in banned_words :
                            if (templine.lower()).find(banned_word)>=0 :
                                banned_word_check = 1
                        # add links to the queue
                        if banned_word_check == 0:
                            weblink_queue.push(templine)
                            weblink_array.append(templine)
        
                # isolate web links using http or https, and check if the link is not a duplicate on the current web page
                elif ("http://" in line or "https://" in line) and (domain_name in line) and (line not in current_page_links) and (line not in weblink_array) :
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
                                weblink_array.append(line)
    
        else: # if webpage is not a homepage, just skip this step
            pass
    else : # if webpage is blank (i.e. the URL was not valid) just skip this step
        pass


""" Function to access a link, parse it for specific types of url's and give out a dictionary output """
def link_grabber(search_words, webpage, website_queue, website_dictionary, visited_links, dictionary, banned_words, search_level):
    
    # initialize if search level = 0 , i.e. home page of website
    if search_level == 0 :
        state_queue = Queue()
        local_queue = Queue()
        allstates_queue = Queue()
        national_queue = Queue()
    
    # if web page is unreachable, skip and return original datasets
    try:
        html_content = urllib2.urlopen(webpage).read()  
    except:
        import sys
        # prints `type(e), e` where `e` is the last exception
        print "Site does not load/exist : ", sys.exc_info()[:2]
        if search_level == 0 :
            return state_queue, local_queue, allstates_queue, national_queue, dictionary
        else :
            return website_dictionary, dictionary
    
    # Search for keywords in a webpage and add them to the dictionary
    for word in search_words :
        matches = re.findall(word, html_content);
        if len(matches) == 0: 
            pass
        else:
            dictionary[webpage] = {}
            dictionary[webpage][word] = len(matches)
            html_content = html_content.replace(word, '<span style="background-color: #FFFF00">'+word+'</span>')
            dictionary[webpage]['content'] = html_content
            print 'Word : ', word, ' - Count : ',len(matches)

    # Browse through website and add all links to website_queue 
    current_page_links = []
    # parse for web links
    for line in re.findall('''href=["'](.[^"']+)["']''', html_content, re.I):
            
        if search_level == 0 :
            # parse for web links
            if (line.find("state-") == 0 or line.find("other-") == 0) and line.find(".cfm") >= 0:
                temp_str = webpage + line
                state_queue.push(temp_str)
            elif line.find("local-") == 0 :
                temp_str = webpage + line
                local_queue.push(temp_str)
            elif line.find("50states-") >= 0 :
                temp_str = webpage + line
                allstates_queue.push(temp_str)
            elif line.find("national_organizations.cfm") == 0 :
                temp_str = webpage + line
                national_queue.push(temp_str)
        
        elif search_level == 1 :
            # isolate web links using http or https, and check if the link is not a duplicate on the current web page
            if ("http://" in line or "https://" in line) and (line not in current_page_links) :
                current_page_links.append(line)
                if (line not in visited_links):
                    # check to see if websites are relevant - remove google searches, wiki articles etc - see banned word list
                    banned_word_check = 0
                    for banned_word in banned_words :
                        if line.find(banned_word)>=0 :
                            banned_word_check = 1
                    if banned_word_check == 0 :
                        print line
                        website_dictionary[webpage].push(line)
    
    # returns different data sets if the page is a home page, or a secondary link
    if search_level == 0 :
        return state_queue, local_queue, allstates_queue, national_queue, dictionary
    if search_level == 1 :
        return website_dictionary, dictionary
    

""" Function to move the contents of a dictionary of queues to a 2-d matrix """   
def move_to_matrix(dictionary):

    matrix = []
    rowcount = 0
    for state in dictionary :
        print rowcount + 1
        while not dictionary[state].isEmpty() :
            row = [state]
            print row
            link = dictionary[state].pop()
            row.append(link)
            matrix.append(row)
        rowcount += 1
    return matrix

""" Function to move the output of a 2-d matrix to a csv file """
def move_to_file(matrix, file):
    with open(file, 'wb') as output:
        writer = csv.writer(output, delimiter=',')
        writer.writerows(matrix)


""" Function to scrape the base website and obtain all links within it - these links will be stored in a file and used later """    
def obtain_all_links(state_file, local_file, allstates_file, national_file, search_words, banned_words) :
    # temporary fix till we get list of web links
    search_level = 0
    
    website_queue = Queue()
    
    #for link in starting_links :
    #    website_queue.push(link)
    website_queue.push('http://www.statelocalgov.net/')
    
    dictionary = {}
    visited_links = []
    
    # Run while website_queue has links - the links get added to the queue when we visit each starting page
    
    # Main page level
    while not website_queue.isEmpty() :
        webpage = website_queue.pop()
        print "Webpage : ", webpage
        if webpage not in visited_links :
            if search_level == 0:
                state_queue, local_queue, allstates_queue, national_queue, dictionary = link_grabber(search_words,webpage,website_queue, [], visited_links, dictionary, banned_words, search_level)    
            visited_links.append(webpage)
            search_level += 1
    
    state_dictionary = {}
    
    # State page level
    while not state_queue.isEmpty() :
        webpage = state_queue.pop()
        print "Webpage : ", webpage
        if webpage not in visited_links :
            if search_level == 1:
                state_dictionary[webpage] = Queue()
                state_dictionary, dictionary = link_grabber(search_words,webpage,state_queue, state_dictionary, visited_links, dictionary, banned_words, search_level)    
            visited_links.append(webpage)
            print state_dictionary
    state_matrix = move_to_matrix(state_dictionary)
    move_to_file(state_matrix, state_file)
    
    local_dictionary = {}
    
    # Local page level
    while not local_queue.isEmpty() :
        webpage = local_queue.pop()
        print "Webpage : ", webpage
        if webpage not in visited_links :
            if search_level == 1:
                local_dictionary[webpage] = Queue()
                local_dictionary, dictionary = link_grabber(search_words,webpage,local_queue, local_dictionary, visited_links, dictionary, banned_words, search_level)    
            visited_links.append(webpage)
    local_matrix = move_to_matrix(local_dictionary)
    move_to_file(local_matrix, local_file)
    
    allstates_dictionary = {}

    # All States page level
    while not allstates_queue.isEmpty() :
        webpage = allstates_queue.pop()
        print "Webpage : ", webpage
        if webpage not in visited_links :
            if search_level == 1:
                allstates_dictionary [webpage] = Queue()
                allstates_dictionary, dictionary = link_grabber(search_words,webpage,allstates_queue, allstates_dictionary, visited_links, dictionary, banned_words, search_level)    
            visited_links.append(webpage)
    allstates_matrix = move_to_matrix(allstates_dictionary)
    move_to_file(allstates_matrix, allstates_file)
    
    national_dictionary = {}
    
    # National page level
    while not national_queue.isEmpty() :
        webpage = national_queue.pop()
        print "Webpage : ", webpage
        if webpage not in visited_links :
            if search_level == 1:
                national_dictionary [webpage] = Queue()
                national_dictionary, dictionary = link_grabber(search_words,webpage,national_queue, national_dictionary, visited_links, dictionary, banned_words, search_level)    
            visited_links.append(webpage)
    
    national_matrix = move_to_matrix(national_dictionary)
    move_to_file(national_matrix, national_file)

""" Function to read contents (links) in the url file, and input them into a queue """
def urlfile_reader(file, column):
    urlqueue = Queue()
    rownum = 0
    starting_links = []
    with open(file,'rb') as readfile:
        reader = csv.reader(readfile)
        for row in reader:
            if rownum == 0 :
                pass
            else :
                if "statelocalgov" not in row[column] :
                    urlqueue.push(row[column]) 
                    starting_links.append(row[column]) 
            rownum = rownum + 1
    
    return urlqueue, starting_links

""" Function to fetch the URL and HTML CONTENT from a given link """
def fetch_url(url):
    if random.randrange(10) <= 1 :
        print "Current Running time : ", datetime.datetime.now() - start
    try :
        print "Opening : ", url
        webpage = urllib2.urlopen(url, timeout=3).geturl()
        html_content = urllib.urlopen(webpage, timeout=3).read()
        return webpage, html_content
    except :
        try :
            webpage = urllib2.urlopen(url,timeout=5).geturl()
            html_content = urllib2.urlopen(webpage,timeout=5).read()
            return webpage, html_content
        except :
            print "Cannot establish connection to URL : ", url
            return "", ""    
        
""" Returns a set of URLs from the queue, in batches of size :size """
def multiple_url_fetcher(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

""" Final save that is done upon exiting the program """
def file_save():
    # save output file
    dir = state_path + '*.csv'    
    with open(state_output, 'wb') as outfile:
        for filename in glob.glob(dir):
            with open(filename) as readfile:
                shutil.copyfileobj(readfile, outfile)              
    print "Created final final : ", datetime.datetime.now(
                                                          )
    # Removing duplicates in final file
    import fileinput
    seen = set() # set for fast O(1) amortized lookup
    for line in fileinput.FileInput(state_output, inplace=1):
        if line in seen: 
            continue # skip duplicate
        seen.add(line)
        print line,       
    print "Removed duplicates from final file : ", datetime.datetime.now()
    
    # save links
    with open(nonparsedlinks_file, 'wb') as output: 
        writer = csv.writer(output, delimiter=',')
        while not weblink_queue.isEmpty():
            row = weblink_queue.pop()
            writer.writerow(row)


def main():
    
    # change to True when you want to restart a run -  this should be done only after an unexpected termination in the previous run
    # the code picks up at the link where the previous run was stopped at
    # DO NOT set this to True if you are running for the first time
    rerun = False
    print "Importing Key word list and Banned word list..."
    
    # Key words to be searched in each web page - modify keywords_list to change the set of words
    workbook = xlrd.open_workbook(keywords_list)
    worksheet = workbook.sheet_by_name('Sheet1')

    rowcount = 1 # starting from 1 to exclude the header
    while rowcount < worksheet.nrows:
        row = worksheet.row(rowcount)
        search_words.append(str(worksheet.cell_value(rowcount, 0)))
        rowcount += 1
    
    # Words to be banned on each web page - modify bannedwords_lists to edit which words should be flagged down
    workbook = xlrd.open_workbook(bannedwords_list)
    worksheet = workbook.sheet_by_name('Sheet1')

    rowcount = 1 # starting from 1 to exclude the header
    while rowcount < worksheet.nrows:
        row = worksheet.row(rowcount)
        banned_words.append(str(worksheet.cell_value(rowcount, 0)))
        rowcount += 1
    
    
    print "Current Running time : ", datetime.datetime.now() - start
    
    # Run this initially to obtain all the links in the base website - skip if this is already done and directly import to data file
    #obtain_all_links(state_file, local_file, allstates_file, national_file, search_words, banned_words)
    
    print "Reading file..."
    
    # checks if the code is a re-run i.e. if there was an unexpected termination in the previous run
    # and we need to restart from that point
    if rerun ==  False :
        weblink_queue, weblink_array = urlfile_reader(state_file, 1) # change according to which file to use
        # creates a list of home pages i.e. base links (and not the parsed URLs)
        for link in weblink_array :
            homepage_array.append(link)
        
    if rerun ==  True :
        weblink_queue, weblink_array = urlfile_reader(nonparsedlinks_file, 0)
     
    
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
            worker_thread = threading.Thread(target=link_scraper, args=(url,))
            worker_thread.start()
            workers.append(worker_thread)
        
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
    
    # performs file save on exit
    atexit.register(file_save)
    
    # main code
    main()

    