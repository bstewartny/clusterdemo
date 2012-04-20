import solr
import math

INDEX_URL='http://localhost:8983/solr'
SEARCH_URL='http://localhost:8983/solr'

index=solr.Solr(INDEX_URL)
search=solr.Solr(SEARCH_URL)

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

threshold=0.30

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

def get_vector(doc):
  freqs=get_tokens(get_text(doc))
  for key in freqs.keys():
    freqs[key]=math.log(freqs[key]+1)

  return freqs

def compute_vector_similarity(a,b):
  # cosine similarity
  top=0.0
  a_bottom=0.0
  b_bottom=0.0
  for token,freq in a.iteritems():
    a_bottom=a_bottom+(freq*freq)
    if b.has_key(token):
      top=top+(freq * b[token])

  for token,freq in a.iteritems():
    b_bottom=b_bottom+(freq * freq)

  return top / (math.sqrt(a_bottom) * math.sqrt(b_bottom))


def compute_similarity(doc,vector):
  return compute_vector_similarity(get_vector(doc),vector)
  
def fetch_similar(doc):
  # fetch MLT using this doc as query
  results=solr.SearchHandler(search,'/mlt')('id:"'+doc['id']+'"')
  return results # maybe need MLT collection here...

temp_clustered={}

def get_cluster_id(doc,modified_docs):
  # get most similar docs using SOLR MLT query
  similar_results=fetch_similar(doc)
  similar_docs=similar_results.results
  top_terms=similar_results.interestingTerms
  vector=get_vector(doc)

  print 'got '+str(len(similar_docs)) +' similar docs...'

  for similar_doc in similar_docs:
    similarity=compute_similarity(similar_doc,vector)
    if similarity >= threshold:
      print 'found cluster, similarity='+str(similarity)
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
