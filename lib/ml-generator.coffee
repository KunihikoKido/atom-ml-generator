fs = require 'fs-plus'
remote = require "remote"
dialog = remote.require "dialog"
{CompositeDisposable} = require 'atom'
{allowUnsafeNewFunction} = require 'loophole'
elasticsearch = allowUnsafeNewFunction -> require 'elasticsearch'

tfidf = (tf, df, n_docs) ->
  idf = Math.log(n_docs / df)
  return tf * idf

config =
  getClassFieldName: ->
    atom.config.get('ml-generator.classFieldName')
  getDocType: ->
    atom.config.get('ml-generator.docType')
  getFieldName: ->
    atom.config.get('ml-generator.fieldName')
  getHost: ->
    atom.config.get('ml-generator.host')
  getIndex: ->
    atom.config.get('ml-generator.index')
  getMaxDocs: ->
    atom.config.get('ml-generator.maxDocs')
  getmaxTerms: ->
    atom.config.get('ml-generator.maxTerms')
  getStatisticsTargetTerms: ->
    atom.config.get('ml-generator.statisticsTargetTerms')
  getIncludeFields: ->
    atom.config.get('ml-generator.includeFields')
  setStatisticsTargetTerms: (terms) ->
    atom.config.set('ml-generator.statisticsTargetTerms', terms)


notifications =
  packageName: 'ML Generator'
  addInfo: (message) ->
    atom.notifications?.addInfo("#{@packageName}: #{message}")
  addError: (message, {detail}={}) ->
    atom.notifications.addError(
      "#{@packageName}: #{message}", detail: detail, dismissable: true)


showSaveDialog = () ->
  getLastOpenPath = ->
    path = localStorage.getItem('ml-generator.lastOpenPath')
    return if path then path else '/undefined.csv'
  setLastOpenPath = (path) ->
    localStorage.setItem('ml-generator.lastOpenPath', path) if path

  fileName = dialog.showSaveDialog(defaultPath: getLastOpenPath())
  setLastOpenPath(fileName)
  return fileName


module.exports = MlGenerator =
  subscriptions: null

  config:
    host:
      type: 'string'
      default: 'http://localhost:9200'
    index:
      type: 'string'
      default: 'blog'
    docType:
      type: 'string'
      default: 'posts'
    fieldName:
      type: 'string'
      default: 'title'
      title: 'Statistics Target Field'
    classFieldName:
      type: 'string'
      default: 'category'
      title: 'Classification Field'
    maxDocs:
      type: 'integer'
      default: 1000
    statisticsTargetTerms:
      type: 'array'
      default: []
    maxTerms:
      type: 'integer'
      default: 50
      description: 'Statistics target terms maximum number of each classification'
    includeFields:
      type: 'array'
      default: []


  activate: (state) ->
    @subscriptions = new CompositeDisposable
    @subscriptions.add atom.commands.add 'atom-workspace', "ml-generator:create-ml-data-source": => @createMlDataSourceCommand()
    @subscriptions.add atom.commands.add 'atom-workspace', "ml-generator:update-statistics-target-terms": => @updateStatisticsTargetTermsCommand()

  deactivate: ->
    @subscriptions.dispose()

  serialize: ->

  Client: ->
    client = new elasticsearch.Client(host: config.getHost())
    return client

  updateStatisticsTargetTermsCommand: ->
    client = @Client()

    options =
      index: config.getIndex()
      type: config.getDocType()
      searchType: 'count'
      body:
        query: match_all: {}
        aggs: classes:
          terms:
            field: config.getClassFieldName()
          aggs: significantClassTerms:
            significant_terms:
              field: config.getFieldName()
              size: config.getmaxTerms()

    client.search(options).then((response) ->
      classBuckets = (response) -> response.aggregations.classes.buckets
      termBuckets = (classBucket) -> classBucket.significantClassTerms.buckets

      terms = []
      for classesBucket in classBuckets(response)
        for termsBukket in termBuckets(classesBucket)
          terms.push(termsBukket.key)
      config.setStatisticsTargetTerms(terms)

    ).catch((error) ->
      notifications.addError("Error update statistics target terms", detail: error)
    ).then(->
      notifications.addInfo("Statistics target terms updated.")
    )

  createMlDataSourceCommand: ->
    isEnabled = ->
      statisticsTargetTerms = config.getStatisticsTargetTerms()
      if statisticsTargetTerms.length is 0
        return false
      return true

    writeCsvHeader = (fileName) ->
      headers = ['id', 'class']
      headers.push(f.replace(/\./g, '_')) for f in config.getIncludeFields()
      terms = config.getStatisticsTargetTerms()
      headers.push("term#{i}") for i in [1..terms.length]
      fs.writeFileSync(fileName, headers.join(',') + '\r\n')

    randomSeed = ({max} = {max: 100}) ->
      Math.floor(Math.random() * max) + 1

    includeFields = ->
      includeFields = config.getIncludeFields()
      includeFields.push(config.getClassFieldName())
      return includeFields

    return notifications.addInfo(
      "Please setup of significant terms at the first.") unless isEnabled()

    fileName = showSaveDialog()
    return unless fileName?

    writeCsvHeader(fileName)

    client = @Client()

    options =
      index: config.getIndex()
      type: config.getDocType()
      searchType: 'scan'
      scroll: '30s'
      body:
        query: filtered:
          query: function_score: random_score: seed: randomSeed()
          filter: exists: field: config.getClassFieldName()
        fields: includeFields()
        sort: _score: 'desc'

    maxDocs = config.getMaxDocs()
    count = 0
    client.search(options).then((response) ->
      return response._scroll_id
    ).then((scrollId) ->
      return client.scroll(scrollId: scrollId, scroll: '30s')
    ).then(getMoreUntileDone = (response) ->
      return if response.hits.hits.length is 0 or count >= maxDocs

      scrollId = response._scroll_id
      count += response.hits.hits.length

      classFieldName = config.getClassFieldName()
      docClasses = {}
      for doc in response.hits.hits
        docClasses[doc._id] = doc.fields[classFieldName].join(';')

      searchDocs = response.hits.hits

      options =
        index: config.getIndex()
        type: config.getDocType()
        body:
          ids: (doc._id for doc in response.hits.hits)
          parameters:
            term_statistics: true
            field_statistics: true
            fields: [config.getFieldName()]

      client.mtermvectors(options).then((response) ->
        getFieldValue = (id, name) ->
          return doc.fields[name].join(';') if doc._id is id for doc in searchDocs

        classFieldName = config.getClassFieldName()
        fieldName = config.getFieldName()
        includeFields = config.getIncludeFields()
        statisticsTargetTerms = config.getStatisticsTargetTerms()

        for tvDoc in response.docs
          items = [tvDoc._id, getFieldValue(tvDoc._id, classFieldName)]
          items.push(getFieldValue(tvDoc._id, name)) for name in includeFields

          field = tvDoc.term_vectors[fieldName]
          for term in statisticsTargetTerms
            weight = 0.0
            if field.terms[term]
              term = field.terms[term]
              tf = term.term_freq
              df = term.doc_freq
              n_docs = field.field_statistics.doc_count
              weight = tfidf(tf, df, n_docs)
            items.push(weight)

          fs.appendFile(fileName, items.join(',') + '\r\n')

        client.scroll(scrollId: scrollId, scroll: '30s').then(getMoreUntileDone)
      )
    ).catch((error) ->
      notifications.addError("Error Create Data Source", detail: error)
    ).then(->
      notifications.addInfo("Data source created.")
    )
