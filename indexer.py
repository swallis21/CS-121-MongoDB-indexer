from bs4 import BeautifulSoup
import nltk
import requests
import json
from collections import defaultdict
from collections import Counter
import sys, os
import math
import logging
import logging.handlers
from urlparse import urlparse
from pymongo import MongoClient
import re

client = MongoClient('52.53.128.88', 27017)
db = client.CS121
db.authenticate('david','parev123', source="admin")

#Static variables
DATABASE_IP = "http://52.53.128.88:5000"

#Initialize Logging for the module
def initialize_logging():
	# Setup Logging
	path = os.path.abspath(__file__)
	dir_path = os.path.dirname(path)

	logger = logging.getLogger(__name__)
	logger.setLevel(logging.INFO)

	# create a file handler
	# Max Log Size - 10 MB
	# Max Log Count - 1
	handler = logging.handlers.RotatingFileHandler((dir_path + '/indexing-new.log'), maxBytes=10 * 1024 * 1024,
												   backupCount=1)
	handler.setLevel(logging.INFO)

	# create a logging format
	formatter = logging.Formatter('%(asctime)s-%(module)-6s: %(levelname)-8s: %(message)s',
								  datefmt='%m/%d/%Y %H:%M:%S ')
	handler.setFormatter(formatter)

	# add the handlers to the logger
	logger.addHandler(handler)
	return logger

def valid_url(url):
	parsed = urlparse(url)
	 # Check for extremely long paths or ridiculously short URLS that may have accidently made their way into the list
	if (len(parsed.path) >= 150 or len(parsed.query) >= 150 or len(url) <= 1):
		return False

	# Check for duplicates in the path of the URL, a common indicator of a crawler trap
	if (re.search("^.*?(\/.+?\/).*?\\1.*$|^.*?\/(.+?\/)\\2.*$", parsed.path) != None):
		return False

	# Extra/superfluous directories can be dynamically generated and trap our crawler
	if (re.search("^.*(\/misc|\/sites|\/all|\/themes|\/modules|\/profiles|\/css|\/field|\/node|\/theme){3}.*$",parsed.path) != None):
		return False

	return True


#Database request methods
def post_request(url, forms):
	succeeded = False
	while not succeeded:
		try:
			serialized = json.dumps(forms)
			response = requests.post(url, data = serialized)
			if response.ok:
				succeeded = True
			else:
				logger.error(u"Error {0} when attempting to POST to {1}.".format(response.status_code, url).encode(sys.stdout.encoding, errors='replace'))
		except Exception as e:
			logger.exception("Exception when trying to post")
	return response

def get_request(url):
	succeeded = False
	while not succeeded:
		try:
			response = requests.get(url, timeout=10)
			if response.ok:
				succeeded = True
				return response
			else:
				logger.error(u"Error {0} when attempting to GET from {1}".format(response.status_code, url).encode(sys.stdout.encoding, errors='replace'))
		except Exception as e:
			print(e)
	
def add_term(term):
	#Delete the term and all associated data if it already exists in the database
	# delete_term(term)
	data = {'term': term, 'docs': []}
	logger.info(u"Adding term {0}".format(term).encode(sys.stdout.encoding, errors='replace'))
	post_request(DATABASE_IP + "/term/add", data)

def delete_term(term):
	logger.info(u"About to delete term {0}".format(term).encode(sys.stdout.encoding, errors='replace'))
	post_request(DATABASE_IP + "/delete", {'terms' : [term]})
	
def add_posting_for_term(term, doc_id, freq):
	data = {'doc_id': doc_id, 'frequency': freq}
	logger.info(u"Adding document {0} for term {1} with a frequency of {2}".format(doc_id,term,freq).encode(sys.stdout.encoding, errors='replace'))
	post_request(DATABASE_IP + u"/{0}/doc/add".format(term), data)

def get_postings_for_term(term):
	postings = get_request(DATABASE_IP + u"/docs/term/{0}".format(term)).text
	logger.info(u"Posting List returned is {0}".format(postings).encode(sys.stdout.encoding, errors='replace'))
	return postings
	
def get_term_list():
	tList = get_request(DATABASE_IP + "/terms").text
	logger.info(u"Term list returned is {0}".format(tList).encode(sys.stdout.encoding, errors='replace'))
	return tList

def update_idf(idf_data):
	post_request(DATABASE_IP + "/idf/update", idf_data)

	
#Page analyzing methods
def pull_bookkeeping():
	#Pull the list of folders and files from the bookkeeping.
	try:
		books = open("bookkeeping.json")
		jsonOutput = json.load(books)
		logger.info(u"Loaded JSON data from bookkeeping: " + str(jsonOutput).encode(sys.stdout.encoding, errors='replace'))
		return jsonOutput
	except IOError:
		logger.error(u"Couldn't find bookkeeping.json. Please place the script in the same folder as it.")

def get_doc_frequencies():
	data = get_request(DATABASE_IP + '/docs/terms/')
	return response.text


def clean_html(soup):
	# remove these tags, complete with contents.
	blacklist = ["script", "style" ]
	# now strip HTML we don't like.
	for tag in blacklist:
		for e in soup(tag):
			e.decompose()
	return soup         

def pull_page_contents(rel_filename):
	#Get all the text using BS4 and tokenize it with NLTK, return list of tokens.
	try:
		page = open(rel_filename)
		contents = BeautifulSoup(page, 'html.parser')
		page.close()
		sentence = ""
		if len(contents.find_all('p')) > 0:
			sentence = contents.find_all('p')[0].get_text()
			sentence = sentence.split('.')[0][:255]
		title = ("" if contents.title == None else contents.title.get_text())
		contents = clean_html(contents)
		tokenized = [word.lower() for word in nltk.tokenize.word_tokenize(contents.get_text()) if word.isalnum() and len(word) <= 25]
		logger.info(u"Tokenized content for filename {0} is {1}".format(rel_filename,tokenized).encode(sys.stdout.encoding, errors='replace'))
		return (tokenized, title, sentence)
	except IOError:
		logger.error(u"Unable to find doc {0} listed in bookkeeping.json.".format(rel_filename).encode(sys.stdout.encoding, errors='replace'))

def freq_analyze(token_list):
	#Return a dictionary with the term frequencies for each token.
	data = Counter(token_list)
	logger.info(u"Returning {0}".format(dict(data)))
	return data

#Main script body

def run_indexer():
	terms = set()
	files = pull_bookkeeping()
	sorted_files = sorted(files)

	validPageCount = 0
	#Sorted purely for our convenience in knowing how far in it is.
	file_data = {}
	doc_data = defaultdict(defaultdict)
	docList = []
	for filename in sorted_files:
		if(valid_url(files[filename]) and not files[filename].endswith('.txt') and not files[filename].endswith('.java') and not files[filename].endswith('.pdf')):
			logger.info(u"Analyzing TF for {0}.".format(filename).encode(sys.stdout.encoding, errors='replace'))
			doc_data[files[filename]] = {'url':files[filename],'terms':{}}
			page_data = pull_page_contents(filename)
			tokens = page_data[0]
			token_frequencies = freq_analyze(tokens)
			for token, frequency in token_frequencies.items():
				docList.append(files[filename])
				doc_data[files[filename]]['terms'][token] = frequency
				logger.info(u"Adding term {0} with frequency {1} to doc_data".format(token,frequency))
				# logger.info(u"Token {0} is not currently been encountered. Adding...".format(token).encode(sys.stdout.encoding, errors='replace'))
				if(token in file_data):
					file_data[token]['docs'].append({'doc_id':files[filename],'frequency':frequency, 'title':page_data[1],'first_sentence':page_data[2]})
				else:
					file_data[token] = {'term':token,'docs':[{'doc_id':files[filename],'frequency':frequency, 'title':page_data[1],'first_sentence':page_data[2]}]}

		else:
			logger.info("Skipping {0} as the URL does not seem valid".format(files[filename]))

	logger.info("Done analyzing term frequencies. Now computing IDF.")
	docList = set(docList)
	logger.info("Total docs in doclist: " + str(len(docList)))
	


	#Request all the terms 

	for k,v in doc_data.items():
			doc_length = 0
			for term, freq in v['terms'].items():
				term_weight = 1 + math.log10(freq)
				doc_length += term_weight * term_weight
			doc_length = math.sqrt(doc_length)
			v['doc_length'] = doc_length
	for k,v in file_data.items():
		idf = math.log10(len(docList)/len(v['docs']))
		logger.info(u"IDF for {0} is {1}.".format(k, idf).encode(sys.stdout.encoding, errors='replace'))
		file_data[k]['idf'] = idf
		for doc in v['docs']:
			doc['tf-idf'] = ((1+math.log10(doc['frequency'])) * (1/doc_data[doc['doc_id']]['doc_length']))
	db.terms.insert_many(file_data.values())
	db.docs.insert_many(doc_data.values())
	logger.info("Indexing is complete.")
	logger.info("Total unique document URLs: " + str(len(docList)))
if __name__ == '__main__':
	logger = initialize_logging()
	run_indexer()

