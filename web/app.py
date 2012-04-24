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


def searchclusters(breadcrumbs,topic,source,category,query,clustered,clusterid):

  handler=get_handler('/searchclusters')
 
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

  for cluster,count in results.facet_counts['facet_fields']['clusterid'].iteritems():
    clusters.append('clusterid:"'+cluster+'"')

  if len(clusters)>0:

    cluster_query=" OR ".join(clusters)

    if len(orig_query)>0:
      cluster_query="(" + orig_query + ") AND ("+cluster_query+")"
  else:
    cluster_query=orig_query

  return search(breadcrumbs,topic,source,category,cluster_query,clustered,clusterid)

def search(breadcrumbs,topic,source,category,query,clustered,clusterid):
  facets=get_entities('*:*')
  categories=get_categories('*:*')
  sources=get_feeds('*:*')

  handler=None
  if clustered:
    handler=get_handler('/searchclustered')
  else:
    handler=get_handler('/searchunclustered')
 
  if clusterid is not None:
    if len(query)>0:
      query='+('+query + ') +clusterid:'+clusterid
    else:
      query='+clusterid:'+clusterid
      
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
  if clustered:
    for group in results.grouped['clusterid']['groups']:
      doclist=group['doclist']
      root=doclist[0]
      clustered_results.append(root)
      if len(doclist)>0:
        root['similar']=doclist[1:]
    results=clustered_results



  return {'results':results,'topics':topics,'sources':sources,'breadcrumbs':breadcrumbs,'categories':categories}


class ClustersHandler(tornado.web.RequestHandler):
  
  def get(self,args):
    
    query=self.get_argument('q','')

    clustered=True

    if self.get_argument('cluster','true')=='false':
      clustered=False
    
    clusterid=self.get_argument('clusterid',None)

    if clusterid is not None:
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


    results=searchclusters(breadcrumbs,topic,source,category,query,clustered,clusterid)

    self.render('templates/clusters.html',query=query,results=results)

class SearchHandler(tornado.web.RequestHandler):
  
  def get(self,args):
    
    query=self.get_argument('q','')

    clustered=True

    if self.get_argument('cluster','true')=='false':
      clustered=False
    
    clusterid=self.get_argument('clusterid',None)

    if clusterid is not None:
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


    results=search(breadcrumbs,topic,source,category,query,clustered,clusterid)

    self.render('templates/index.html',query=query,results=results)

class MoreLikeThisHandler(tornado.web.RequestHandler):

  def get(self):
    id=self.get_argument('id')
    mlt_results=get_handler('/mlt')('id:"'+id+'"')

   
    results=search([],None,None,None,'id:"'+id+'"',False,None)

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
              (r"/clusters/(.*)",ClustersHandler),
              (r"/mlt",MoreLikeThisHandler),
              (r"/(.*)", SearchHandler)],
              
              static_path=os.path.join(os.path.dirname(__file__),"static")
              )

if __name__ == "__main__":
  application.listen(8888)
  tornado.ioloop.IOLoop.instance().start()
