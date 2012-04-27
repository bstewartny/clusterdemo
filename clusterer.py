import solr
import math
import re
import nltk

INDEX_URL='http://localhost:8983/solr'
SEARCH_URL='http://localhost:8983/solr'

index=solr.Solr(INDEX_URL)
search=solr.Solr(SEARCH_URL)

threshold=0.20

entity_df={}

ngram_df={}

total_count=100000

def get_entity_idf(entity):
  global entity_df
  df=entity_df.get(entity,2)
  return get_idf(df)

def get_idf(df):
  global total_count
  return math.log(1 + (total_count / df))

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

stop_words_array="how,can,corp,inc,llp,llc,inc,v,v.,vs,vs.,them,he,she,it,where,with,now,legal,can,how,new,may,from,not,did,you,does,any,why,your,are,llp,corp,inc,a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with"

stop_words={}

for word in stop_words_array.split(','):
  stop_words[word]=word

for word in nltk.corpus.stopwords.words('english'):
  stop_words[word]=word

def fetch_similar_docs(entities,ngrams):
  clauses=[]
  for entity in entities:
    clauses.append('entity:"'+entity+'"')

  for ngram in ngrams:
    clauses.append('ngram:"'+ngram+'"')

  query=' OR '.join(clauses)

  print query
  return solr.SearchHandler(search,'/fetchsimilardocs')(query).results

def compute_similarity(a,b):
  return compute_vector_similarity(get_vector(a),get_vector(b))

def get_vector(a):
  vector={}
  for entity in get_entities(a):
    vector[entity]=get_entity_idf(entity)
  for ngram in get_ngrams(a):
    vector[ngram]=get_ngram_idf(ngram)

  return vector

def compute_vector_similarity(a,b):
  
  if len(a)==0 or len(b)==0:
    return 0.0

  top=0.0
  
  bottoma=0.0
  
  for feature,weight in a.iteritems():
    top=top + weight * b.get(feature,0.0)
    bottoma=bottoma+(weight*weight)

  if top==0.0:
    return 0.0

  bottomb=0.0

  for feature,weight in b.iteritems():
    bottomb=bottomb+(weight*weight)
  
  if bottomb==0.0:
    return 0.0

  bottom=math.sqrt(bottoma) * math.sqrt(bottomb)
  
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
  total_count=get_total_count()
  ngram_df=get_ngram_counts()
  entity_df=get_entity_counts()
  results=execute_search_handler('/unclustered')
  cluster_docs(results.results)
