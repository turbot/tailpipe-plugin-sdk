# Artifact Discovery - Path Walking and Filtering

## Basics

The `file_layout` property specifies the layout of the files in the artifact, using [grok](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html#:~:text=Grok%20works%20by%20combining%20text%20patterns%20into%20something%20that%20matches%20your%20logs.&text=The%20SYNTAX%20is%20the%20name,matched%20by%20the%20IP%20pattern) syntax.

Using this, we can extract metadata from the file path, such as `account_id`, `region`, `year`, `month`, `day`. This metadata is used for various things:
- storing 'collection state', i.e. keeping track of which files have been processed already for a given source
- discovering new files to process, by walking the file system and filtering files based on the metadata

For both of these usages, we need to be able to apply a filter to a path segment. 
This will be used during artifact discovery when we walk the folder tree.

Here is the basic flow.

```go
func WalkTreeNode(path, fileLayout string filters []Filter){
    // get child folders
    folders = getFolders(path)
    
    // for each chekc whether we should travers
    for _, folder := range folders {
        if !pathSegmentSatisfiesFilters(folder, fileLayout, filters) {
            continue
        }
        WalkTreeNode(folder, filters)
    } 
    
    // todo find leaf nodes and process them
}

```

So - what is needed is the `pathSegmentSatisfiesFilters` function (or whatever we call it) . This function will take a path segment, the file layout, and a list of filters, and return whether the path segment should be traversed or not.


```go

func pathSegmentSatisfiesFilters(pathSegment, fileLayout string, filters []Filter) bool {
    // split the layout into path segments and reconstruct to match the length of the path segment
    pathLength = len(strings.Split(pathSegment, "/"))
    fileLayout = strings.Join(strings.Split(fileLayout, "/")[:pathLength], "/")
    
    // extract metadata from the path segment
    metadata = extractMetadata(pathSegment, fileLayout)
    
    // apply filters
    for _, filter := range filters {
        if !filter.Apply(metadata) {
            return false
        }
    }
    return true
}
```

-> Can we use SQL format filtering?