import tornado.ioloop
import tornado.web
import feeds
import solr
import os
from tornado.template import Template
import simplejson
import operator

SOLR_URL='http://localhost:8983/solr'
client=solr.Solr(SOLR_URL) 

def get_handler(name):
  return solr.SearchHandler(client,name)



def get_topic_sources(topickey):
  return []

def get_topic_topics(topickey):
  return []

def get_topics():
  entitysearch=get_handler('/entities')
  entity_results=entitysearch('*:*')
  # for each entity, get top sources...
  return [{'name':key,'key':feeds.create_slug(key),'sources':get_topic_sources(feeds.create_slug(key)),'topics':get_topic_topics(feeds.create_slug(key))} for key,value in entity_results.facet_counts['facet_fields']['entity'].iteritems()]

def get_sources():
  return []

def get_entities(query):
  entitysearch=get_handler('/entities')
  entity_results=entitysearch(query)
  return [key for key,value in entity_results.facet_counts['facet_fields']['entity'].iteritems()]

def get_feeds(query):
  feedssearch=get_handler('/feeds')
  feeds_results=feedssearch(query)
  return [key for key,value in feeds_results.facet_counts['facet_fields']['feed'].iteritems()]

def get_categories(query):
  feedssearch=get_handler('/categories')
  feeds_results=feedssearch(query)
  return [key for key,value in feeds_results.facet_counts['facet_fields']['category'].iteritems()]


def searchclusters(breadcrumbs,topic,source,category,query,clustered,handler,clusterfieldname):

  #handler=get_handler('/searchclusters')
 
  # get the top clusters for this query
  orig_query=query

  if (topic is not None) or (source is not None) or (category is not None):
    if len(query)>0:
      query='+('+query+')'
    if category is not None:
      query=query + ' +categorykey:'+category
    if topic is not None:
      query=query + ' +entitykey:'+topic
    if source is not None:
      query=query + ' +feedkey:'+source

  results=handler(query)

  # get top clusters (biggest ones...)
  # create a new query using those clusters only...
  clusters=[]

  for cluster,count in results.facet_counts['facet_fields'][clusterfieldname].iteritems():
    clusters.append(clusterfieldname+':"'+cluster+'"')

  if len(clusters)>0:

    cluster_query=" OR ".join(clusters)

    if len(orig_query)>0:
      cluster_query="(" + orig_query + ") AND ("+cluster_query+")"
  else:
    cluster_query=orig_query

  results=search(breadcrumbs,topic,source,category,cluster_query,True,clusterfieldname)

  # sort by cluster size - largest first

  sorted_results=results['results']
  
  sorted_results=sorted(sorted_results,key=lambda r: len(r.get('similar',[])))

  sorted_results.reverse()

  results['results']=sorted_results

  return results


def searchcarrot(breadcrumbs,topic,source,category,query,clustered):
  facets=get_entities('*:*')
  categories=get_categories('*:*')
  sources=get_feeds('*:*')

  handler=get_handler('/searchcarrotclustered')
 
  if (topic is not None) or (source is not None) or (category is not None):
    if len(query)>0:
      query='+('+query+')'
    if category is not None:
      query=query + ' +categorykey:'+category
    if topic is not None:
      query=query + ' +entitykey:'+topic
    if source is not None:
      query=query + ' +feedkey:'+source

  results=handler(query)

  topics=[{'name':facet,'key':feeds.create_slug(facet)} for facet in facets]
  sources=[{'name':source,'key':feeds.create_slug(source)} for source in sources]
  categories=[{'name':source,'key':feeds.create_slug(source)} for source in categories]
  
  clustered_results=[]
  doc_map={}
  for result in results.results:
    doc_map[result['id']]=result

  max_docs_per_cluster=8
  
  for carrot_cluster in results.clusters:
    docids=carrot_cluster['docs']
    docs=[]
    for docid in docids:
      docs.append(doc_map[docid])  
      if len(docs)>=max_docs_per_cluster:
        break
    label=" ".join(carrot_cluster['labels'])
    clustered_results.append({'label':label,'docs':docs})

  return {'clusters':clustered_results,'topics':topics,'sources':sources,'breadcrumbs':breadcrumbs,'categories':categories}

def search(breadcrumbs,topic,source,category,query,clustered,clusterfieldname):
  facets=get_entities('*:*')
  categories=get_categories('*:*')
  sources=get_feeds('*:*')

  handler=None
  if clustered:
    handler=get_handler('/searchclustered')
  else:
    handler=get_handler('/searchunclustered')
 
  if (topic is not None) or (source is not None) or (category is not None):
    if len(query)>0:
      query='+('+query+')'
    if category is not None:
      query=query + ' +categorykey:'+category
    if topic is not None:
      query=query + ' +entitykey:'+topic
    if source is not None:
      query=query + ' +feedkey:'+source

  results=handler(query)

  topics=[{'name':facet,'key':feeds.create_slug(facet)} for facet in facets]
  sources=[{'name':source,'key':feeds.create_slug(source)} for source in sources]
  categories=[{'name':source,'key':feeds.create_slug(source)} for source in categories]

  entities=[{'name':key,'key':key} for key,value in results.facet_counts['facet_fields']['entity'].iteritems()]

  clustered_results=[]
  if clustered:
    for group in results.grouped[clusterfieldname]['groups']:
      doclist=group['doclist']
      root=doclist[0]
      clustered_results.append(root)
      if len(doclist)>0:
        root['similar']=doclist[1:]
    results=clustered_results

  return {'results':results,'entities':entities  ,'topics':topics,'sources':sources,'breadcrumbs':breadcrumbs,'categories':categories}


class CarrotHandler(tornado.web.RequestHandler):
 
  def get(self,args):
    
    query=self.get_argument('q','')

    source=None
    
    topic=None
    
    category=None

    if args is not None and len(args)>0:
      if args[:1]=='/':
        args=args[1:]

    parts=args.split('/')
    
    if len(parts)>1:
      if parts[0]=='topic':
        topic=parts[1]
        if len(parts)==4:
          source=parts[3]
      elif parts[0]=='source':
        source=parts[1]
        if len(parts)==4:
          topic=parts[3]
      elif parts[0]=='category':
        category=parts[1]
        if len(parts)==4:
          source=parts[3]
  
    breadcrumbs=[]
    if len(parts)>1:
      breadcrumbs.append({'name':parts[1],'link':'/'+parts[0]+'/'+parts[1],'active':False})
      if len(parts)==4:
        breadcrumbs.append({'name':parts[3],'link':'/'+parts[0]+'/'+parts[1]+'/'+parts[2]+'/'+parts[3],'active':False})
    if len(query)>0:
      breadcrumbs.append({'name':query,'link':self.request.uri,'active':False})

    if len(breadcrumbs)>0:
      breadcrumbs[len(breadcrumbs)-1]['active']=True

    results=searchcarrot(breadcrumbs,topic,source,category,query,True)

    self.render('templates/carrot.html',query=query,results=results)

class MahoutHandler(tornado.web.RequestHandler):
 
  def get(self,args):
    
    query=self.get_argument('q','')

    source=None
    
    topic=None
    
    category=None

    if args is not None and len(args)>0:
      if args[:1]=='/':
        args=args[1:]

    parts=args.split('/')
    
    if len(parts)>1:
      if parts[0]=='topic':
        topic=parts[1]
        if len(parts)==4:
          source=parts[3]
      elif parts[0]=='source':
        source=parts[1]
        if len(parts)==4:
          topic=parts[3]
      elif parts[0]=='category':
        category=parts[1]
        if len(parts)==4:
          source=parts[3]
  
    breadcrumbs=[]
    if len(parts)>1:
      breadcrumbs.append({'name':parts[1],'link':'/'+parts[0]+'/'+parts[1],'active':False})
      if len(parts)==4:
        breadcrumbs.append({'name':parts[3],'link':'/'+parts[0]+'/'+parts[1]+'/'+parts[2]+'/'+parts[3],'active':False})
    if len(query)>0:
      breadcrumbs.append({'name':query,'link':self.request.uri,'active':False})

    if len(breadcrumbs)>0:
      breadcrumbs[len(breadcrumbs)-1]['active']=True

    handler=get_handler('/searchmahoutclusters')
    clusterfieldname="mahoutclusterid"
    results=searchclusters(breadcrumbs,topic,source,category,query,True,handler,clusterfieldname)

    self.render('templates/mahout.html',query=query,results=results)

class ClustersHandler(tornado.web.RequestHandler):
 
  def get(self,args):
    
    query=self.get_argument('q','')

    source=None
    
    topic=None
    
    category=None

    if args is not None and len(args)>0:
      if args[:1]=='/':
        args=args[1:]

    parts=args.split('/')
    
    if len(parts)>1:
      if parts[0]=='topic':
        topic=parts[1]
        if len(parts)==4:
          source=parts[3]
      elif parts[0]=='source':
        source=parts[1]
        if len(parts)==4:
          topic=parts[3]
      elif parts[0]=='category':
        category=parts[1]
        if len(parts)==4:
          source=parts[3]
  
    breadcrumbs=[]
    if len(parts)>1:
      breadcrumbs.append({'name':parts[1],'link':'/'+parts[0]+'/'+parts[1],'active':False})
      if len(parts)==4:
        breadcrumbs.append({'name':parts[3],'link':'/'+parts[0]+'/'+parts[1]+'/'+parts[2]+'/'+parts[3],'active':False})
    if len(query)>0:
      breadcrumbs.append({'name':query,'link':self.request.uri,'active':False})

    if len(breadcrumbs)>0:
      breadcrumbs[len(breadcrumbs)-1]['active']=True

    handler=get_handler('/searchclusters')
    clusterfieldname="clusterid"
    results=searchclusters(breadcrumbs,topic,source,category,query,True,handler,clusterfieldname)

    self.render('templates/index.html',query=query,results=results)

class SearchHandler(tornado.web.RequestHandler):
  
  def get(self,args):
    
    query=self.get_argument('q','')

    clustered=True

    if self.get_argument('cluster','true')=='false':
      clustered=False
    

    source=None
    
    topic=None
    
    category=None

    parts=args.split('/')
    
    if len(parts)>1:
      if parts[0]=='topic':
        topic=parts[1]
        if len(parts)==4:
          source=parts[3]
      elif parts[0]=='source':
        source=parts[1]
        if len(parts)==4:
          topic=parts[3]
      elif parts[0]=='category':
        category=parts[1]
        if len(parts)==4:
          source=parts[3]

  
    breadcrumbs=[]
    if len(parts)>1:
      breadcrumbs.append({'name':parts[1],'link':'/'+parts[0]+'/'+parts[1],'active':False})
      if len(parts)==4:
        breadcrumbs.append({'name':parts[3],'link':'/'+parts[0]+'/'+parts[1]+'/'+parts[2]+'/'+parts[3],'active':False})
    if len(query)>0:
      breadcrumbs.append({'name':query,'link':self.request.uri,'active':False})

    if len(breadcrumbs)>0:
      breadcrumbs[len(breadcrumbs)-1]['active']=True


    results=search(breadcrumbs,topic,source,category,query,clustered,'clusterid')

    self.render('templates/index.html',query=query,results=results)

class MoreLikeThisHandler(tornado.web.RequestHandler):

  def get(self):
    id=self.get_argument('id')
    mlt_results=get_handler('/mlt')('id:"'+id+'"')

   
    results=search([],None,None,None,'id:"'+id+'"',False,'clusterid')

    if len(results['results'].results)>0:
      match=results['results'].results[0]
    else:
      match=None

    results['results']=mlt_results

    self.render('templates/mlt.html',match=match,query='',results=results,interestingTerms=mlt_results.interestingTerms)


class TopicsHandler(tornado.web.RequestHandler):

  def get(self):
    topics=get_topics()
    self.render('templates/topics.html',topics=topics)

class SourcesHandler(tornado.web.RequestHandler):

  def get(self):
    sources=get_sources()
    self.render('templates/sources.html',sources=sources)

class CategoriesHandler(tornado.web.RequestHandler):

  def get(self):
    sources=get_sources()
    self.render('templates/categories.html',sources=sources)

class AutoSuggestHandler(tornado.web.RequestHandler):

  def get(self):
    prefix=self.get_argument('term','')
    terms_client=get_handler('/terms')
    results=terms_client(terms_regex=prefix+'.*')
    json=[{'id':term,'label':term,'value':'"'+term+'"'} for term in sorted(results.terms['entity'].keys())]
    self.content_type = 'application/json'
    self.write(simplejson.dumps(json))

application = tornado.web.Application([
              (r"/autosuggest",AutoSuggestHandler),
              (r"/topics",TopicsHandler),
              (r"/sources",SourcesHandler),
              (r"/categories",CategoriesHandler),
              (r"/clusters(.*)",ClustersHandler),
              (r"/mahout(.*)",MahoutHandler),
              (r"/carrot(.*)",CarrotHandler),
              (r"/mlt",MoreLikeThisHandler),
              (r"/(.*)", ClustersHandler)],
              
              static_path=os.path.join(os.path.dirname(__file__),"static")
              )

if __name__ == "__main__":
  application.listen(8888)
  tornado.ioloop.IOLoop.instance().start()
