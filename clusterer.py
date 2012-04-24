import solr
import math
import re

INDEX_URL='http://localhost:8983/solr'
SEARCH_URL='http://localhost:8983/solr'

index=solr.Solr(INDEX_URL)
search=solr.Solr(SEARCH_URL)

top_feed_words={}

top_index_words_map={} #None

def get_unclustered_docs():
  # get all doc IDs from SOLR that are not clustered
  results=solr.SearchHandler(search,'/unclustered')()
  return results.results

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

threshold=0.10

def update_doc(doc):
  index.add(doc,commit=False)

def index_commit():
  index.commit()

def compute_total(top_terms):
  total=0.0
  for key,weight in top_terms.iteritems():
    total+=weight
  return total

def get_text(doc):
  text=doc['title']+' '+doc['title']+' '+doc['title']

  #if doc.has_key('summary'):
  #  text=text+' '+doc['summary']
  #if doc.has_key('body'):
  #  text=text+' '+doc['body']
  return text

def get_tokens(text):
  freqs={}
  for token in text.strip().lower().split():
    if len(token)>3:
      freqs[token]=freqs.get(token,0)+1
  return freqs


stop_words_array="how,can,corp,inc,llp,llc,inc,v,v.,vs,vs.,them,he,she,it,where,with,now,legal,can,how,new,may,from,not,did,you,does,any,why,your,are,llp,corp,inc,a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with"

stop_words={}
for word in stop_words_array.split(','):
  stop_words[word]=word


def get_vector(doc):

  text=doc['clustertext']

  text=" ".join(text)

  words=re.split('\W+',text.strip())

  # remove stop words
  words=[word for word in words if not stop_words.has_key(word.lower())]

  # remove non-upper-case words
  words=[word for word in words if word[:1].upper()==word[:1]]

  shingles=[[" ".join(words[i:i+n]) for i in range(len(words)- n+1)] for n in range(2,3)]

  flattened=[shingle for sublist in shingles for shingle in sublist]

  freqs={}
  for shingle in flattened:
    freqs[shingle]=freqs.get(shingle,0)+1
  
  for key in freqs.keys():
    freqs[key]=math.log(freqs[key] * len(key))

  return freqs

def print_vector(v):
  print ", ".join(v.keys())

def compute_vector_similarity(a,b):
  # cosine similarity

  if a is None:
    return 0.0

  if b is None:
    return 0.0

  print_vector(a)
  print_vector(b)



  if len(a)<3:
    return 0.0

  if len(b)<3:
    return 0.0

  top=0.0
  a_bottom=0.0
  b_bottom=0.0
  for token,freq in a.iteritems():
    a_bottom=a_bottom+(freq*freq)
    if b.has_key(token):
      top=top+(freq * b[token])

  for token,freq in a.iteritems():
    b_bottom=b_bottom+(freq * freq)

  if(a_bottom>0 and b_bottom>0):

    return top / (math.sqrt(a_bottom) * math.sqrt(b_bottom))
  else:
    return 0.0

def compute_similarity(doc,vector):
  return compute_vector_similarity(get_vector(doc),vector)
  
def fetch_similar(doc):
  # fetch MLT using this doc as query
  results=solr.SearchHandler(search,'/mlt')('id:"'+doc['id']+'"')
  return results # maybe need MLT collection here...

temp_clustered={}

def fetch_docs_with_good_words(words):

  clauses=[]

  for word in words:
    clauses.append('title:"'+word+'"')

  query=" OR ".join(clauses)

  return solr.SearchHandler(search,'/fetchsimilardocs')(query).results

def split_words(text):
  return re.split('\W+',text.strip().lower())

def get_word_freqs_from_results(results):
  words={}
  for doc in results.results:
    for word in split_words(doc['title']):
      if len(word)>2:
        words[word]=words.get(word,0)+1
  return words

def get_top_feed_words(feed):
  if top_feed_words.has_key(feed):
    return top_feed_words[feed]
  # search for a bunch of headlines for a feed...
  results=solr.SearchHandler(search,'/feeddocs')('feed:"'+feed+'"')
  if len(results.results)<10:
    top_feed_words[feed]={}
    return {}

  words=get_word_freqs_from_results(results)
  # return any words in more than 50% of documents
  r=map_array([word for word,freq in words.iteritems() if freq>=len(results.results)/2.0])
  top_feed_words[feed]=r
  if len(r)>0:
    print "============================"
    print 'top feed words: '+str(r)
  return r

def get_top_index_words():
  global top_index_words_map
  if len(top_index_words_map)>0: # is not None:
    return top_index_words_map
  results=solr.SearchHandler(search,'/indexdocs')()
  words=get_word_freqs_from_results(results)
  # return any words in more than 10% of documents
  r=map_array([word for word,freq in words.iteritems() if freq>=results.numFound/10.0])
  top_index_words_map=r
  print "============================"
  print 'top index words: '+str(r)
  return r

def map_array(a):
  d={}
  for word in a:
    d[word]=word
  return d

def remove_words(words,map):
  return [word for word in words if not map.has_key(word.lower())]

def is_alpha(word):
  return word[:1].isalpha()

def get_good_words(doc):
  # get bad words for feed
  bad_feed_words=get_top_feed_words(doc['feed'])
 
  bad_index_words=get_top_index_words()

  doc_words=split_words(doc['title'])

  doc_words=[word for word in doc_words if len(word)>2]

  doc_words=[word for word in doc_words if is_alpha(word)]

  # remove any stop words
  if len(stop_words)>0:
    doc_words=remove_words(doc_words,stop_words)

  # remove any top feed words
  if len(bad_feed_words)>0:
    doc_words=remove_words(doc_words,bad_feed_words)

  # remove any top words
  if len(bad_index_words)>0:
    doc_words=remove_words(doc_words,bad_index_words)

  return doc_words

def is_uppercase(word):
  return word[:1].upper()==word[:1]

def is_uppercase_set(s):
  for i in range(len(s)):
    if not is_uppercase(s.pop()):
      return False
  return True

def has_good_overlap(a,b):
  
  if a is None or len(a)<3: 
    return False
  
  if b is None or len(b)<3:
    return False
  
  matches=set(a).intersection(set(b))
  
  min_size=min(len(a),len(b))
  max_size=max(len(a),len(b))

  min_matches=max(3,max_size/2.0)
  
  min_matches=max(3,min(min_size,min_matches))
  
  if len(matches)>=min_matches:
    print str(matches)
    return True
  
  return False

def get_cluster_id(doc,modified_docs):
  
  good_words=get_good_words(doc)

  if good_words is None or len(good_words)<2:
    return doc['clusterid']

  # find other docs with any of these good words
  similar_docs=fetch_docs_with_good_words(good_words)

  #print "found "+str(len(similar_docs))+" similar docs"

  # see if any of them have at least a minimum number of good words
  for similar_doc in similar_docs:
    if similar_doc['id']==doc['id']:
      continue
    
    #if similar_doc['title']==doc['title']:
    #  return similar_doc['clusterid']
    similar_good_words=get_good_words(similar_doc)
    if has_good_overlap(good_words,similar_good_words):
      print doc['title']+' ==> ' +similar_doc['title']
      return similar_doc['clusterid']

  return doc['clusterid']

def get_cluster_id_old(doc,modified_docs):
  # get most similar docs using SOLR MLT query
  similar_results=fetch_similar(doc)
  similar_docs=similar_results.results
  top_terms=similar_results.interestingTerms
  vector=get_vector(doc)

  title=doc['title']

  if title[:5]=="Case:":
    return doc['clusterid']
  
  if title[:17]=="Chamber judgement":
    return doc['clusterid']
  
  #print 'got '+str(len(similar_docs)) +' similar docs...'
  
  for similar_doc in similar_docs:
    similar_doc_title=similar_doc['title']
    
    if similar_doc_title[:5]=='Case:':
      continue

    if similar_doc_title[:17]=="Chamber judgement":
      continue
    
    if similar_doc_title==title:
      similarity=100.0
    else:
      similarity=compute_similarity(similar_doc,vector)
    
    if similarity >= threshold:
      #print 'found cluster, similarity='+str(similarity)
      print doc['title'] + " === " + similar_doc['title'] + ' ('+str(similarity)+')'
      if similar_doc['clustered']=='false':
        if modified_docs.has_key(similar_doc['id']):
          prev_similar_doc=modified_docs[similar_doc['id']]
          return prev_similar_doc['clusterid']
        else:
          # we need to mark this document to be updated
          similar_doc['clustered']='true'
          modified_docs[similar_doc['id']]=similar_doc
      # join document to this cluster
      return similar_doc['clusterid']
    else:
      return doc['clusterid']

if __name__=="__main__":
  cluster_docs(get_unclustered_docs())
