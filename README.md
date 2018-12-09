# Extra Morphline commands

The following [Morhline](http://kitesdk.org/docs/1.1.0/morphlines/morphlines-reference-guide.html) commands implemented here: 

## containsRegex
Similar to the original [contains](http://kitesdk.org/docs/1.1.0/morphlines/morphlines-reference-guide.html#contains) but also regexp is allowed. 
example: 
```
{
   if {
      conditions : [
        { containsRegex { "id" : ".*2$" } }
      ]
      then : [
     {
       addValues {
        contains : "true"
     }
     }
     ] 
     else : [
       {
         addValues {
         contains : "false"
       }
       }
     ]
     }
}
```

## conditionalDrop
Drops the current record if all the listed properties matches the provided regexp. A property may have a list of values in morphlines. If any of those value matches, the property matches.
```
    {
  conditionalDrop {
   boardId : "customer-tkb"
   depth: "(?!(0)).*"
  }
  }
```

## extractJsonPathsFromField
Copied from the original [extractJsonPaths](http://kitesdk.org/docs/1.1.0/morphlines/morphlines-reference-guide.html#extractJsonPaths), which parses the json from the Fields.ATTACHMENT_BODY field. It is possible to provide the **sourceStringField** for extractJsonPathsFromField that contains the json. Other than this the extractJsonPathsFromField works exactly the same as the original extractJsonPaths.

```
{ 
        extractJsonPathsFromField {
          sourceStringField: "fullcontent"
          flatten:  true
            paths: {
              content: "/orig/body"
              teaser: "/orig/teaser"
              title: "/orig/subject/"
              id: "/orig/id"
              view_href: "/orig/id"
              boardId: "/orig/board/id"
              depth: "/orig/depth"
            }
        }
       }
```

## htmlProcessor
Populate record from HTML strings, that are defined in the _source field, using a CSS selector. By default it grabs the html element content. If the optional ';' is given: after ';' attribute name is xml expected and its attribute value is returned.
```
  {
  htmlProcessor {
   _source: source
   content_facets : "meta[name=facet];content"
   title : "title"
  }
  }
```

## dispatchingLoadSolr
Inserts the record into an already existing Solr collection. The collection name to insert the current record to should be  given at the **targetCollection** field for every record. The **collection** field of the solrLocator must point to a collection, whose schema is identical to the targetCollection. Nothing will be inserted into this latter collection.

```
{
        dispatchingLoadSolr {
   solrLocator : {
     collection : collectionName
      zkHost : "$ZK_HOST"
           batchSize : 10000
        }
     }
     }
```
