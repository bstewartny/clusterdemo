from BeautifulSoup import BeautifulSoup
import feedparser
import solr
import time
import datetime
import feeddefs
#import urllib2
import urllib3

import re
from readability.readability import Document 
import subprocess
import nltk
import pytz
from threading import Thread
from nltk.stem.porter import PorterStemmer

http=urllib3.PoolManager()

INDEX_URL='http://localhost:8983/solr'

existing_doc_ids={}

stemmer=PorterStemmer()

stopwords={}

client=solr.Solr(INDEX_URL)

def parse_case_title(title):
  title=title.strip()
  # remove common prefix/suffix
  # asfsfds [ORDER]
  if title[-7:]=='[ORDER]':
    title=title[:-7]
    title=title.strip()

  # asfafdf (10-875)
  if title[-1:]==')':
    i=title.find('(')
    if i>0:
      title=title[:i]
      title=title.strip()

  # Case: adsfasdfdasf
  if title[:5]=='Case:':
    title=title[5:]
    title=title.strip()

  # adsfasdf 10-55643
  # 11-60500 asdfasdfasdf
  # 11-5017 | asdfasfdsfd

  # plaintiff v defendant
  # plaintiff v. defendant
  parts=title.split(' V. ')
  if len(parts)!=2:
    parts=title.split(' v. ')
  if len(parts)!=2:
    parts=title.split(' V ')
  if len(parts)!=2:
    parts=title.split(' v ')
    
  plaintiff=None
  defendent=None
  if len(parts)==2:
    plaintiff=parts[0].strip()
    defendent=parts[1].strip()
  
  if plaintiff is None or defendent is None:
    return None
  else:
    return {'plaintiff':plaintiff,'defendant':defendent}

def update_doc_entities():
  results=solr.SearchHandler(client,'/unclustered')()
  print 'Got '+str(len(results.results)) +' docs'
  for result in results.results:
    result['clusterid']=result['id']
    result['mahoutclusterid']=result['id']
    result['entity']=None #get_entities(result['title'])
    ngrams=get_ngrams(result['title'],2,4)
    if ngrams is None or len(ngrams)<2:
      #print 'failed to get ngrams for doc: '+result['title']
      ngrams=[result['id']]
    result['ngram']=ngrams
    d=parse_case_title(result['title'])
    if d is not None:
      result['defendant']=d['defendant']
      result['plaintiff']=d['plaintiff']
      
    client.add(result,commit=False)
  client.commit()

for word in nltk.corpus.stopwords.words('english'):
  stopwords[word]=1

def get_ngrams(text,min,max):
  global stemmer
  global stopwords

  lower=text.strip().lower()

  lower=lower.replace(" v "," versus ").replace(" v. "," versus ").replace(" vs "," versus ").replace(" vs. "," versus ")
  
  # break into words
  words=nltk.word_tokenize(text.strip().lower())
  
  # strip out stop words
  words=[word for word in words if not stopwords.has_key(word)]

  # strip out single-char words (including lots of punctuation)
  words=[word for word in words if len(word)>1]

  #TODO: remove punctuation

  # stem remaining words
  words=[stemmer.stem_word(word) for word in words]

  # remove numbers?
  words=[word for word in words if not word.isdigit()]

  # remove non alpha-numeric words
  words=[word for word in words if word.isalnum()]
  
  # if all words are not uppercase, and some words are all uppercase then they can be single terms if >= 3 chars long...


  ngrams=[]
  
  # get ngrams where min<=n<=max
  for n in range(min,max+1):
    ngrams.extend(' '.join(a) for a in nltk.ngrams(words,n))
  
  if min>1:
    if not text.upper()==text:
      shorts=[word.lower() for word in nltk.word_tokenize(text.strip()) if word.isalpha() and len(word)==4 and word.upper()==word]
      if len(shorts)==1:
        print 'Adding short upper case terms: '+str(shorts)
        ngrams.extend(shorts)


  # remove duplicates
  return list(set(ngrams))

def get_all_doc_ids():
  ids={}

  results=solr.SearchHandler(index,'/getids')()

  for result in results.results:
    ids[result['id']]=result['id']

  return ids

index=solr.Solr(INDEX_URL)

invalid_entities=[] #['tweet text',

#invalid_entities.extend([feed['feed'].lower() for feed in feeddefs.feeds])

def extract_entity_names(t):
  entity_names = []
  if hasattr(t, 'node') and t.node:
      if t.node == 'NE':
          entity_names.append(' '.join([child[0] for child in t]))
      else:
          for child in t:
              entity_names.extend(extract_entity_names(child))
              
  return entity_names

def is_substr(s,t):
  if len(t)<len(s):
    return False
  else:
    t=t.lower()
    return ((not t==s) and (t.startswith(s) or t.endswith(s)))

def has_substr(s,a):
  s=s.lower()
  for t in a:
    if is_substr(s,t):
      return True
  return False

def filter_entities(a):
  u=[]
  a=list(set(a))
  # remove any single-word entities (too ambiguous in most cases)
  #a=[s for s in a if len(s.split())>1]
  # remove any entities more than 2 words
  a=[s for s in a if len(s.split())<3]
  #a.sort(lambda x,y:cmp(len(y),len(x)))
  #a=[x for x in a if not has_substr(x,a)]
  a=[s for s in a if not s.lower() in invalid_entities]
  return a

def get_entities(text):
  
  case=parse_case_title(text)
  if case is not None:
    return [case['defendant'],case['plaintiff']]
  
  
  sentences = nltk.sent_tokenize(text)
  tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
  tagged_sentences = [nltk.pos_tag(sentence) for sentence in tokenized_sentences]
  chunked_sentences = nltk.batch_ne_chunk(tagged_sentences, binary=True)
  
  entity_names=[]
  for tree in chunked_sentences:
    entity_names.extend(extract_entity_names(tree))

  return filter_entities(entity_names)

def get_page_text(url):
  txt=''
  if url[-4:]=='.pdf':
    return ''
  data=None
  try:
    print 'Get url:',url
    r=http.request('GET',url)
    data=r.data
    #data = urllib2.urlopen(url).read()
    if not data is None and len(data)>0:
      summary=Document(data).summary()
      return strip_html_tags(summary)
  except:
    print 'error getting text from url: '+url
    #if not data is None and len(data)>0:
    #  return get_text_lynx(data)
  return txt

bad_chars="'+-&|!(){}[]^\"~*?:\\"

def replace_bad_chars(s):
  for c in bad_chars:
    s=s.replace(c,'_')
  return s

def compress_underscores(s):
  while '__' in s:
    s=s.replace('__','_')
  return s

slug_pattern=re.compile('[\W_]+')

def create_slug(s):
  # create url friendly name for source/topic name
  return slug_pattern.sub('',s)

def create_id_slug(s):
  # strip out non-URL and non-Lucene friendly stuff...
  return compress_underscores(replace_bad_chars(s.strip().replace('http://','').replace('https://','')))	

def strip_html_tags(html):
  # just get appended text elements from HTML
  try:
    text="".join(BeautifulSoup(html,convertEntities=BeautifulSoup.HTML_ENTITIES).findAll(text=True))
    return text
  except:
    print 'Failed to strip html tags...'
    return html

def get_attribute(item,names):
  for name in names:
    if name in item:
      return item[name]
  return None

utc=pytz.utc

def get_item_date(item):
  d=get_attribute(item,['published_parsed','updated_parsed','created_parsed','date_parsed'])
  if not d is None:
    try:
      return datetime.datetime.fromtimestamp(time.mktime(d)).replace(tzinfo=utc)
    except:
      return None
  return None

max_summary_len=400
min_sentence_len=20
max_sentence_len=300
min_clean_words=6
min_clean_words_ratio = 0.8
min_word_len=2
max_word_len=25

def clean_sentence(sent):
  # remove date line
  # TODO: also look for unicode 'long dash' here...
  m=re.match('^.+--\s',sent)
  if m is not None:
    sent=sent[m.end():]
  return sent

def is_clean_word(word):
  if len(word)>max_word_len:
    return False
  if len(word)<min_word_len:
    return False
  if re.match('^[a-z]+$',word) is None and re.match('^[A-Z][a-z]+$',word) is None:
    return False
  return True

def is_clean_sentence(sent):
  if len(sent) < min_sentence_len:
    return False
  if len(sent) > max_sentence_len:
    return False
  if not sent[0].upper()==sent[0]:
    return False
  if not sent[-1]=='.':
    return False
  words=nltk.word_tokenize(sent)
  num_clean_words=len(filter(is_clean_word,words))
  if num_clean_words < min_clean_words:
    return False
  if float(num_clean_words) / float(len(words)) < min_clean_words_ratio:
    return False
  return True

def clean_summary(summary):
  # get one or two very clean sentences from the text
  if summary is None:
    return None
  if len(summary)==0:
    return None
  clean_sentences=[]
  total_len=0
  summary=summary.replace('&apos;','\'')
  for sentence in filter(is_clean_sentence,map(clean_sentence,nltk.sent_tokenize(summary))):
    if total_len+len(sentence) > max_summary_len and len(clean_sentences)>0:
      return ' '.join(clean_sentences)
    else:
      clean_sentences.append(sentence)
      total_len=total_len+len(sentence)
  
  if len(clean_sentences)>0:
    return ' '.join(clean_sentences)
  else:
    if len(summary)>200:
      return summary[:200]+'...'
    else:
      return summary

def analyze_feed_item(item):

  item["id"]=create_id_slug(get_attribute(item,["link","title"]))
 
  if existing_doc_ids.has_key(item['id']):
    return None
  else:
    existing_doc_ids[item['id']]=item['id']

  # strip HTML tags from summary/description
  summary=get_attribute(item,['summary','description'])
  
  if not summary is None:
    summary=strip_html_tags(summary)
    item["summary"]=summary #clean_summary(summary)

  link=item['link']
  text=summary
  #if len(link)>0 and (summary is not None) and len(summary)<210:
  #  text=get_page_text(link)
  #  if text is None or len(summary)>len(text):
  #    text=summary
  #  item["body"]=text
  #else:
  item["body"]=summary

  #if (text is not None) and ((not item.has_key('summary')) or (item["summary"] is None) or (len(item["summary"])==0)):
  #  item["summary"]=text[:200] #clean_summary(text)
    
  #text = item["title"] + " "+text
  #print 'get_entities'
  #text=item['title']+' '+text

  #entities=get_entities(item['title']) #text)
  #item['entity']=entities
  
  ngrams=get_ngrams(item['title'],2,4)
  if ngrams is None or len(ngrams)<2:
    ngrams=[result['id']]
  item['ngram']=ngrams
  
  return item

def copy_attribute(source,dest,name):
  if source.has_key(name):
    if not dest.has_key(name):
      dest[name]=source[name]

def copy_attributes(source,dest,names):
  for name in names:
    copy_attribute(source,dest,name)

def normalize_title(title):
  return create_slug(title)

def create_solr_doc(item):
  doc={}
  copy_attributes(item,doc,['id','title','summary','body','author','feed','feedlink','link','entity','entitykey'])
  d=get_item_date(item)
  doc['feedkey']=create_slug(doc['feed'])
  doc['clustered']='false'
  clusterid=normalize_title(doc['title'])
  doc['clusterid']=clusterid
  doc['mahoutclusterid']=clusterid
  if d is not None:
    doc["date"]=d
  return doc

def index_doc(doc):
  index.add(doc,commit=False)

def index_error_queue():
  for doc in queue:
    index.add(doc,commit=False)

def index_commit():
  index.commit()

def process_item(feed,item):
  
  copy_attributes(feed,item,['feed','name','rss'])
  item=analyze_feed_item(item)
  if item is not None:
    doc=create_solr_doc(item)
    if item.has_key('category'):
      doc['category']=item['category']
      doc['categorykey']=create_slug(doc['category'])
    else:
      if feed.has_key('category'):
        doc['category']=feed['category']
        doc['categorykey']=create_slug(doc['category'])
        
    index_doc(doc)

def process_feed(feed):
  print 'processing feed: '+feed['feed'] +'...'
  rss=feedparser.parse(feed['rss'])
  count=0
  for item in rss["items"]:
    process_item(feed,item)
    count=count+1
  print 'processed '+str(count)+' items from feed: '+feed['rss']
  return count

def process_feeds(feeds):
  total=0
  print 'processing '+str(len(feeds))+' feeds...'
  for feed in feeds:
    total=total+process_feed(feed)
    index_commit()
  
  index_commit()
  print 'processed '+str(total)+' total items.'


num_threads=4

if __name__ == "__main__":
  feeds=feeddefs.feeds
  batch_size=len(feeds)/num_threads
  existing_doc_ids=get_all_doc_ids()
  print 'Got '+str(len(existing_doc_ids)) + ' existing doc ids'
  threads=[]
  for i in range(0,num_threads):
    batch=feeds[i*batch_size:(i*batch_size)+batch_size]
    thread=Thread(target=process_feeds,args=(batch,))
    thread.start()
    threads.append(thread)

  #process_feeds(feeddefs.feeds)

