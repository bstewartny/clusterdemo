import solr
import math
import re
import nltk
import os

INDEX_URL='http://localhost:8983/solr'
SEARCH_URL='http://localhost:8983/solr'

index=solr.Solr(INDEX_URL)
search=solr.Solr(SEARCH_URL)

# similarity threshold which is the minimum percentage of similarity in order to cluster documents together
threshold=0.175 #0.20 #175

# cache of entity document frequencies, used to calculate IDF used in feature weights
entity_df={}

# cache of ngram document frequencies, used to calculate IDF used in feature weights
ngram_df={}

# the total number of documents in the index, used to calculate IDF used in feature weights
total_count=100000

def mahout_command(name,params):
  os.system('export JAVA_HOME=/usr; /usr/local/mahout/bin/mahout '+name+' '+params)

def mahout_lucene_vectors():
  mahout_command('lucene.vector','-d /Users/bstewart/clusterdemo/solr/data/index/ -o lucene.vectors -t dict -x 50 -w TFIDF -f ngram --idField id --norm 2 -err 0.05')

def mahout_canopy(t1,t2):
  mahout_command('canopy','-i lucene.vectors -o canopy-output -dm org.apache.mahout.common.distance.CosineDistanceMeasure -t1 '+str(t1)+' -t2 '+str(t2) + ' -ow -cl')

def mahout_kmeans(k,maxIter):
  mahout_command('kmeans','-c kmeans-centroids -k '+str(k)+' -cd 0.1 -cl -ow -i lucene.vectors -o kmeans-output --maxIter '+str(maxIter))

def mahout_kmeans_post_canopy(maxIter):
  mahout_command('kmeans','-c kmeans-centroids -cd 0.1 -cl -ow -i canopy-output/clusters-0/ -o kmeans-output --maxIter '+str(maxIter))

def mahout_clusterdump_kmeans():
  mahout_command('clusterdump','-p kmeans-output/clusteredPoints/ -d dict -of CSV -s kmeans-output/clusters-*-final/ -n 0 > clusters.csv')

def mahout_clusterdump_canopy():
  mahout_command('clusterdump','-p canopy-output/clusteredPoints/ -d dict -of CSV -s canopy-output/clusters-0/ -n 0 > clusters.csv')

def mahout_readclusters():
  clusters={}
  lines=open('clusters.csv','r').readlines()
  print 'read '+str(len(lines)) +' clusters from file'
  for line in lines:
    parts=line.split(',')
    clusterid='mahout'+parts[0]
    if len(parts)>1:
      for i in range(1,len(parts)):
        clusters[parts[i]]=clusterid

  return clusters

def update_doc_clusterids(docs,clusters):
  print 'update docs'
  for doc in docs:
    try:
      if clusters.has_key(doc['id']):
        doc['mahoutclusterid']=clusters[doc['id']]
      else:
        doc['mahoutclusterid']=doc['id']
    except:
      print 'failed to find cluster for id: '+doc['id']

def mahout_clusters_canopy_kmeans(docs):
  print 'get lucene vectors'
  mahout_lucene_vectors()
  print 'do mahout canopy'
  mahout_canopy(0.7,0.75)
  mahout_kmeans_post_canopy(10)
  print 'dump clusters'
  mahout_clusterdump_kmeans()
  print 'read clusters'
  clusters=mahout_readclusters()
  update_doc_clusterids(docs,clusters)

def mahout_clusters_canopy(docs):
  print 'get lucene vectors'
  mahout_lucene_vectors()
  print 'do mahout canopy'
  mahout_canopy(0.7,0.8)
  print 'dump clusters'
  mahout_clusterdump_canopy()
  print 'read clusters'
  clusters=mahout_readclusters()
  update_doc_clusterids(docs,clusters)

def mahout_clusters_kmeans(docs):
  print 'get lucene vectors'
  mahout_lucene_vectors()
  print 'do mahout kmeans'
  mahout_kmeans(2000,10)
  print 'dump clusters'
  mahout_clusterdump_kmeans()
  print 'read clusters'
  clusters=mahout_readclusters()
  update_doc_clusterids(docs,clusters)

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


def get_distance(a,b):
  
  tmp=0.0
  for key,weight in a.iteritems():
    diff=weight - b.get(key,0.0)
    tmp=tmp+(diff*diff)
  return math.sqrt(tmp)
  
  #return 1.0 - compute_vector_similarity(a,b)

def cluster_docs_kmeans(docs):
  print 'kmeans clustering '+str(len(docs))+' unclustered docs...'
  vectors=[get_vector(doc) for doc in docs]
  clusters=kmeans(vectors,get_distance,max(100,len(docs) / 100))
  
  clusterid=0
  for cluster in clusters:
    for docid in cluster:
      doc=docs[docid]
      doc['clusterid']='cluster'+str(clusterid)
    clusterid=clusterid+1
  
  print 'updating '+str(len(docs))+' docs...'
  for doc in docs:
    update_doc(doc)
  index_commit()

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
  #print 'updating '+str(len(modified_docs))+' docs...'
  #for uid,doc in modified_docs.iteritems():
  #for doc in docs:
  #  update_doc(doc)
  #index_commit()

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
def fetch_similar_docs(entities,ngrams,defendant,plaintiff):
  clauses=[]

  if defendant is not None:
    clauses.append('defendant:"'+defendant+'"^10.0')

  if plaintiff is not None:
    clauses.append('plaintiff:"'+plaintiff+'"^10.0')

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
  if a['title']==b['title']:
    return 100.0
  if a.get('defendant',True)==b.get('defendant',False):
    return 100.0
  if a.get('plaintiff',True)==b.get('plaintiff',False):
    return 100.0

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

  defendant=doc.get('defandant',None)
  plaintiff=doc.get('plaintiff',None)

  # find other docs with any of these good words
  similar_docs=fetch_similar_docs(entities,ngrams,defendant,plaintiff)

  # see if any of them have at least a minimum number of good words
  for similar_doc in similar_docs:
    if similar_doc['id']==doc['id']:
      continue
    similarity=compute_similarity(doc,similar_doc)
    if similarity>=threshold:
      if similarity < 100.0:
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


import random


def kmeans(vectors,distance,k):
  # collect the dimensions (features) from docs
  dims={}
  print 'gather all dimensions from vectors'
  for v in vectors:
    for key,weight in v.iteritems():
      rang=dims.get(key,[weight,weight])
      rang[0]=min(rang[0],weight)
      rang[1]=max(rang[1],weight)
      dims[key]=rang

  min_max=3.0
  orig_size=len(dims)
  print 'eliminate smaller dimensions'
  dims=dict([(key,rang) for key,rang in dims.iteritems() if rang[1]>min_max])

  print 'new size='+str(len(dims)) +', orig size='+str(orig_size)
  
  print 'gathered '+str(len(dims))+' dimensions'

  print 'create '+str(k) +' random centroids'
  # create random centroids

  print 'get ranges'
  ranges=[(key,rang) for key,rang in dims.iteritems()]

  print 'get centroids'
  clusters=[dict([(key,random.random() *(rang[1]-rang[0]) + rang[0]) for (key,rang) in ranges]) for j in range(k)]

  lastmatches=None
  for t in range(100):
    print 'Iteration: %d' % t
    bestmatches=[[] for i in range(k)]

    # find centroid closest to each doc
    for j in range(len(vectors)):
      vector=vectors[j]
      bestmatch=0
      bestdistance=distance(vector,clusters[bestmatch])
      for i in range(k):
        d=distance(vector,clusters[i])
        if d<bestdistance:
          bestmatch=i
          bestdistance=d
      # add document to list of items in cluster
      bestmatches[bestmatch].append(j)
    
    # if results are same as last time, we are done
    if bestmatches==lastmatches:
      print 'Reached convergence...'
      break
    lastmatches=bestmatches

    print 'Compute average centroids...'
    # move centroid to average of their members
    for i in range(k):
      avg={}
      # get documents in cluster
      clustermatches=bestmatches[i]
      for j in clustermatches:
        vector=vectors[j]
        for key,weight in vector.iteritems():
          avg[key]=avg.get(key,0.0)+weight
      avg=dict([(key,weight/len(clustermatches)) for key,weight in avg.iteritems()])
      clusters[i]=avg
  
  return lastmatches

if __name__=="__main__":
  # get total count of documents from index
  total_count=get_total_count()
  print 'total_count='+str(total_count)
  # get ngram document frequencies from index
  ngram_df=get_ngram_counts()
  # get entity document frequencies from index
  entity_df=get_entity_counts()
  results=execute_search_handler('/unclustered')
  #mahout_clusters_kmeans(results.results)
  mahout_clusters_canopy_kmeans(results.results)

  #cluster_docs_kmeans(results.results)
  cluster_docs(results.results)
  for result in results.results:
    update_doc(result)
  index_commit()
