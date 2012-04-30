import solr
import math
import re
import nltk

INDEX_URL='http://localhost:8983/solr'
SEARCH_URL='http://localhost:8983/solr'

index=solr.Solr(INDEX_URL)
search=solr.Solr(SEARCH_URL)

# similarity threshold which is the minimum percentage of similarity in order to cluster documents together
threshold=0.20

# cache of entity document frequencies, used to calculate IDF used in feature weights
entity_df={}

# cache of ngram document frequencies, used to calculate IDF used in feature weights
ngram_df={}

# the total number of documents in the index, used to calculate IDF used in feature weights
total_count=100000

def get_idf(df):
  global total_count
  return math.log(1 + (total_count / df))

def get_entity_idf(entity):
  global entity_df
  df=entity_df.get(entity,2)
  return get_idf(df)

def get_ngram_idf(ngram):
  global ngram_id
  df=ngram_df.get(ngram,2)
  return get_idf(df)

def get_total_count():
  results=execute_search_handler('/totalcount')
  return results.numFound

def get_facet_counts(results,name):
  return results.facet_counts['facet_fields'][name]

def execute_search_handler(name):
  return solr.SearchHandler(search,name)()

def get_ngram_counts():
  return get_facet_counts(execute_search_handler('/ngramcount'),'ngram')

def get_entity_counts():
  return get_facet_counts(execute_search_handler('/entitycount'),'entity')

def cluster_docs(docs):
  print 'clustering '+str(len(docs))+' unclustered docs...'
  modified_docs={}
  for doc in docs:
    if not modified_docs.has_key(doc['id']):
      cluster_id=get_cluster_id(doc,modified_docs)
      if cluster_id is not None:
        doc['clusterid']=cluster_id
      doc['clustered']='true'
      modified_docs[doc['id']]=doc
  
  # update all docs that now have cluster ID
  print 'updating '+str(len(modified_docs))+' docs...'
  for uid,doc in modified_docs.iteritems():
    update_doc(doc)
  index_commit()

def update_doc(doc):
  index.add(doc,commit=False)

def index_commit():
  index.commit()

# setup stop words map using common english words and some other noise...
stop_words_array="how,can,corp,inc,llp,llc,inc,v,v.,vs,vs.,them,he,she,it,where,with,now,legal,can,how,new,may,from,not,did,you,does,any,why,your,are,llp,corp,inc,a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with"

stop_words={}

for word in stop_words_array.split(','):
  stop_words[word]=word

for word in nltk.corpus.stopwords.words('english'):
  stop_words[word]=word

# find most similar docs in the index
def fetch_similar_docs(entities,ngrams):
  clauses=[]
  # build query using document entities
  for entity in entities:
    clauses.append('entity:"'+entity+'"')

  # build query using document ngrams
  for ngram in ngrams:
    clauses.append('ngram:"'+ngram+'"')

  # do query for documents containing any of the entities or ngrams
  query=' OR '.join(clauses)

  return solr.SearchHandler(search,'/fetchsimilardocs')(query).results

def compute_similarity(a,b):
  return compute_vector_similarity(get_vector(a),get_vector(b))

def get_vector(a):
  vector={}
  # build vector using entity features
  for entity in get_entities(a):
    vector[entity]=get_entity_idf(entity)
  # build vector using ngram features
  for ngram in get_ngrams(a):
    vector[ngram]=get_ngram_idf(ngram)
  return vector

# compute similarity between vectors using cosine similarity
def compute_vector_similarity(a,b):
  
  if len(a)==0 or len(b)==0:
    return 0.0

  top=0.0
  
  bottoma=0.0
  
  # compute dot product (intersection of the two documents feature vectors)
  for feature,weight in a.iteritems():
    # add intersecting features (will just add 0 if no match for this feature)
    top=top + weight * b.get(feature,0.0)
    # compute magnitude of vector a
    bottoma=bottoma+(weight*weight)

  if top==0.0:
    return 0.0

  bottomb=0.0

  for feature,weight in b.iteritems():
    # compute magnitude of vector b
    bottomb=bottomb+(weight*weight)
  
  if bottomb==0.0:
    return 0.0

  # bottom part is the cross-product (magnitude of the two vectors)
  bottom=math.sqrt(bottoma) * math.sqrt(bottomb)
  
  # return cosine measure as a percentage of similarity
  return top / bottom

def get_values(doc,name):
  if doc.has_key(name):
    return doc[name]
  else:
    return []

def get_entities(doc):
  return get_values(doc,'entity')

def get_ngrams(doc):
  return get_values(doc,'ngram')

def get_cluster_id(doc,modified_docs):

  entities=get_entities(doc)
  ngrams=get_ngrams(doc)
  if len(entities)==0 and len(ngrams)==0:
    return doc['clusterid']

  # find other docs with any of these good words
  similar_docs=fetch_similar_docs(entities,ngrams)

  # see if any of them have at least a minimum number of good words
  for similar_doc in similar_docs:
    if similar_doc['id']==doc['id']:
      continue
    similarity=compute_similarity(doc,similar_doc)
    if similarity>=threshold:
      try:
        print "============================"
        print "similarity: "+str(similarity)
        print doc['title']
        print similar_doc['title']
        print "============================"
      except:
        print 'tried to print title but not ascii i guess...'
      return similar_doc['clusterid']
  return doc['clusterid']

if __name__=="__main__":
  # get total count of documents from index
  total_count=get_total_count()
  # get ngram document frequencies from index
  ngram_df=get_ngram_counts()
  # get entity document frequencies from index
  entity_df=get_entity_counts()
  results=execute_search_handler('/unclustered')
  cluster_docs(results.results)
