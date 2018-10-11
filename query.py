from pymongo import MongoClient
import time
from sklearn.metrics.pairwise import cosine_similarity
import numpy
import math
import copy




def connectToDb(user,passw,host='52.53.128.88',port=27017,authDb='admin'):
	client = MongoClient(host, port)
	db = client.CS121
	db.authenticate(user,passw,source=authDb)
	return db


def calculateQueryTFIDF(query_terms):
    queryTFIDF = {}
    for x in copy.deepcopy(query_terms):
        docs = list(db.terms.find({'term':x}))
        if(len(docs) > 0):
            queryTFIDF[x] = docs[0]['idf']
        else:
            queryTFIDF[x] = 0
    return queryTFIDF

def getDocs(terms):
	data = list(db.terms.aggregate([
	{'$match': 
		   {'term': {'$in': terms}}
	},
	{'$unwind' : "$docs"},
	{ 
		'$group': { 
			'_id': {
				'url': '$docs.doc_id'            },
			'x': { 
					'$push': { 'term': '$term','weight' :'$docs.tf-idf'}
				  }
		}
	}
	]))
	data = [{'doc_id':d['_id']['url'],'terms':{t['term']:t['weight'] for t in d['x']}} for d in data]
	return data

def removeGenericTerms(terms):
	if(len(terms) == 1):
		return terms

	docs = list(db.terms.aggregate([
		{'$match': {'term': {'$in': terms}}},
		]))
	if(len(docs) > 0):
		generic_terms = sorted([(d['term'],d['idf']) for d in docs if d['idf'] < 1],key=lambda x:x[1])
		for t in generic_terms:
			if len(terms) - 1 > 0:
				terms.remove(t[0])
	print("Returning {0}".format(terms))
	return terms


'''def getTermsByDoc(urls):

	query = []
	start = time.time()
	for term in terms:
		query.append({'terms.'+term: {'$exists':1}})
	data = list(db.docs.aggregate([{'$match':{'$or':query}},{'$project': {'doc_id':'$url','terms':'$terms'}}]))
	print("time {0}".format(time.time() - start))
	return data'''







def vectorize(data):
	vectorized = {}
	for d in data:
		vectorized[d['doc_id']] = {}
		docVector = []
		for t in d['terms']:
			vectorized[d['doc_id']][t] = d['terms'][t]

	return vectorized



db = connectToDb('david','parev123')
query = raw_input("Enter your input query:")

start = time.time()
terms = [term.lower() for term in query.split(' ')] #removeGenericTerms([term.lower() for term in query.split(' ')])
term_weights = calculateQueryTFIDF(terms)

normalized_weights = {}
query_length = 0
for term,weight in term_weights.items():
	query_length += weight * weight
query_length = math.sqrt(query_length)
#print("QUERY LENGTH: " + str(query_length))


for term, weight in term_weights.items():
	normalized_weights[term] = (1/query_length) * weight
#print("Normalized Weights: " + str(normalized_weights))




docs = getDocs(terms)
start = time.time()
print("Total urls to pull: " + str(len(docs)))
#Gives us a dictionary keyed by URL with a dictionary nested that is keyed by the term to get the weight
doc_normalized_weights = vectorize(docs)
print(str(time.time() - start))

scores = {}
for term,weight in normalized_weights.items():
    for doc in docs:
        if not doc['doc_id'] in scores:
            scores[doc['doc_id']] = 0
        product = 0
        if term in doc_normalized_weights[doc['doc_id']]:
            product = doc_normalized_weights[doc['doc_id']][term] * weight
        scores[doc['doc_id']] += product

scores = sorted([(k,v) for k,v in scores.items()],key=lambda x:x[1],reverse=True)
for i in scores[:10]:
    print("URL: " + str(i[0]) + "\tScore: " + str(i[1]))

