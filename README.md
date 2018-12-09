# Homegrown Morphline commands

The following [Morhline](http://kitesdk.org/docs/1.1.0/morphlines/morphlines-reference-guide.html) commands implemented here: 

##containsRegex

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

##conditionalDrop

Drops the current record if all the listed properties matches the provided regexp. As a property may have a list of values, if any of the list members matches, the property matches.
```
	   {
		conditionalDrop {
			boardId : "customer-tkb"
			depth: "(?!(0)).*"
		}
		}
```

##extractJsonPathsFromField
Copied from the original [extractJsonPaths](http://kitesdk.org/docs/1.1.0/morphlines/morphlines-reference-guide.html#extractJsonPaths), which uses the Fields.ATTACHMENT_BODY to parse the json. You can provide the **sourceStringField** for extractJsonPathsFromField for the input json. Other than that it works exactly the same as the original.

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
Css selector for HTML files: Grabs the xml element content. ';' is optional. After ';' attribute name is xml expected.
```
		{
		htmlProcessor {
			_source: source
			content_facets : "meta[name=facet];content"
			title : "title"
		}
		}
```


##dispatchingLoadSolr
Inserts the record into a predefined collection. The collection name to insert the current record to is given at the "targetCollection" for every record. 

```
{
        dispatchingLoadSolr {
			solrLocator : {
  			# Name of solr collection. Nothing will be inserted into this collection. Its schema must be identical to the overrider collection name.
  			collection : collectionName
		    zkHost : "$ZK_HOST"
  	        batchSize : 10000
        }
	    }
	    }
```