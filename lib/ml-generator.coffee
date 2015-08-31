fs = require 'fs-plus'
remote = require "remote"
dialog = remote.require "dialog"
{CompositeDisposable} = require 'atom'
{allowUnsafeNewFunction} = require 'loophole'
elasticsearch = allowUnsafeNewFunction -> require 'elasticsearch'

config =
  getCategoryField: ->
    atom.config.get('ml-generator.categoryField')
  getDocType: ->
    atom.config.get('ml-generator.docType')
  getStatisticsTargetField: ->
    atom.config.get('ml-generator.statisticsTargetField')
  getHost: ->
    atom.config.get('ml-generator.host')
  getIndex: ->
    atom.config.get('ml-generator.index')
  getMaxDocs: ->
    atom.config.get('ml-generator.maxDocs')
  getStatisticsMaxTerms: ->
    atom.config.get('ml-generator.statisticsMaxTerms')
  getStatisticsTargetTerms: ->
    atom.config.get('ml-generator.statisticsTargetTerms')
  isStatisticsEnabled: ->
    atom.config.get('ml-generator.statisticsEnabled')
  getIncludeFields: ->
    includes = [atom.config.get('ml-generator.categoryField')]
    for field in atom.config.get('ml-generator.includeFields')
      includes.push(field)
    return includes
  setStatisticsTargetTerms: (terms) ->
    atom.config.set('ml-generator.statisticsTargetTerms', terms)


notifications =
  packageName: 'ML Generator'
  addInfo: (message, {detail}={}) ->
    atom.notifications?.addInfo("#{@packageName}: #{message}", detail: detail)
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


elasticsearchClient = () ->
  client = new elasticsearch.Client(host: config.getHost())
  return client


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
    categoryField:
      type: 'string'
      default: 'category'
      description: 'Categoraization field of elasticsearch'
    maxDocs:
      type: 'integer'
      default: 10000
    statisticsEnabled:
      type: 'boolean'
      default: true
    statisticsTargetField:
      type: 'string'
      default: 'title'
      description: 'Statistics target field of elasticsearch'
    statisticsTargetTerms:
      type: 'array'
      default: []
      description: 'Setup manual or Run `Update Statistics Target Terms` command.'
    statisticsMaxTerms:
      type: 'integer'
      default: 50
      description: 'Statistics target terms maximum number of each categories'
    includeFields:
      type: 'array'
      default: []


  activate: (state) ->
    @subscriptions = new CompositeDisposable
    @subscriptions.add atom.commands.add 'atom-workspace', "ml-generator:create": => @createCommand()
    @subscriptions.add atom.commands.add 'atom-workspace', "ml-generator:update-statistics-target-terms": => @updateStatisticsTargetTermsCommand()

  deactivate: ->
    @subscriptions.dispose()

  serialize: ->

  updateStatisticsTargetTermsCommand: ->
    client = elasticsearchClient()

    options =
      index: config.getIndex()
      type: config.getDocType()
      searchType: 'count'
      body:
        query: filtered:
          query: match_all: {}
          filter: exists: field: config.getCategoryField()
        aggs: categories:
          terms:
            field: config.getCategoryField()
          aggs: significantTerms:
            significant_terms:
              field: config.getStatisticsTargetField()
              size: config.getStatisticsMaxTerms()

    client.search(options).then((response) ->
      terms = []
      for category in response.aggregations.categories.buckets
        for term in category.significantTerms.buckets
          terms.push(term.key)
      config.setStatisticsTargetTerms(terms)
      return terms
    ).catch((error) ->
      notifications.addError(
        "Error update statistics target terms", detail: error)
    ).then((terms)->
      notifications.addInfo(
        "Statistics target terms updated.",
        detail: "#{terms[..10]}... #{terms.length} terms")
    )

  createCommand: ->
    isEnabled = ->
      return config.getStatisticsTargetTerms().length isnt 0

    writeCsvHeader = (fileName) ->
      headers = ['id']
      for field in config.getIncludeFields()
        headers.push(field)
      if config.isStatisticsEnabled()
        field = config.getStatisticsTargetField()
        for i in [1..config.getStatisticsTargetTerms().length]
          headers.push("#{field}.term#{(("000") + i).substr(-3)}")
      fs.writeFileSync(fileName, headers.join(',') + '\r\n')

    return notifications.addInfo(
      "Please setup of significant terms at the first.") unless isEnabled()

    fileName = showSaveDialog()
    return unless fileName?

    writeCsvHeader(fileName)

    client = elasticsearchClient()

    options =
      index: config.getIndex()
      type: config.getDocType()
      searchType: 'scan'
      scroll: '30s'
      fielddataFields: config.getIncludeFields()
      body:
        query: filtered:
          query: match_all: {}
          filter: exists: field: config.getCategoryField()

    count = 0
    client.search(options).then((response) ->
      return response._scroll_id
    ).then((scrollId) ->
      return client.scroll(scrollId: scrollId, scroll: '30s')
    ).then(getMoreUntileDone = (response) ->
      makeIncludeFieldData = (docs) ->
        fieldsData = {}
        for doc in docs
          fieldsData[doc._id] = {}
          for field in config.getIncludeFields()
            values = if doc.fields[field] then doc.fields[field] else []
            fieldsData[doc._id][field] = values.join(' ')
        return fieldsData

      return if response.hits.hits.length is 0 or count >= config.getMaxDocs()

      count += response.hits.hits.length
      scrollId = response._scroll_id

      includeFieldsData = makeIncludeFieldData(response.hits.hits)

      options =
        index: config.getIndex()
        type: config.getDocType()
        body:
          ids: (doc._id for doc in response.hits.hits)
          parameters:
            term_statistics: true
            field_statistics: true
            fields: [config.getStatisticsTargetField()]

      client.mtermvectors(options).then((response) ->
        for doc in response.docs
          items =[doc._id]
          for field in config.getIncludeFields()
            items.push(includeFieldsData[doc._id][field])

          if config.isStatisticsEnabled()
            for field, vectors of doc.term_vectors
              for term in config.getStatisticsTargetTerms()
                term_statistics = vectors.terms[term]
                if term_statistics
                  items.push(term_statistics.term_freq)
                else
                  items.push(0)

          fs.appendFileSync(fileName, items.join(',') + '\r\n')

        client.scroll(scrollId: scrollId, scroll: '30s').then(getMoreUntileDone)
      )

      return fileName
    ).catch((error) ->
      notifications.addError("Error Create Data Source", detail: error)
    ).then((fileName) ->
      notifications.addInfo("Data source created.", detail: fileName)
    )
