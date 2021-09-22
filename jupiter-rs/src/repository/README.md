# File Repository

The repository contains a set of files which is stored on the local disk. This can be populated via two
ways:

1) Files can be updated/added/removed in the "repository" directory manually and the `REPO.SCAN` command
can be used to make Jupiter aware of changed files.
2) Files can be loaded using `REPO.FETCH` or `REPO.STORE` and deleted via `REPO.DELETE`. This can e.g. be
used to sync files form an upstream S3 bucket which contains shared master data.
   
Once files are updated, Jupiter will check if an appropriate **loader** is available to be invoked. This
loader then determines how the data is to be processed if it is changed or deleted.

All loaders must reside in the sub directory *loaders* and are configured using YAML files. 

Note that **indices** are used to speedup exact value lookup (like **IDB.LOOKUP**) and **fulltextIndices**
speedup searches like **IDB.SEARCH**. Note that for both indices, values in child lists or child maps
are also added to the index. For exact indices, if a child map is indexed, a sub-index for each entry is
created.

Therefore when indexing "mappings" for:
```yaml 
mappings:
    acme: test
```

We'd create an exact index match for "mappings -> test", another one for "mappings.acme -> test" and
inexact one for "mappings -> test".

Here are some examples for the built-in loaders provided by Jupiter:

## Loading a CSV file into an InfoGraphDB table:

Data:
```csv
name;value;unit
test;10;PCE
foo;5;MTR
```

Loader:
```yaml
file: 'path/to/file.csv'
loader: 'idb-csv'
namespace: 'target namespace to import into'
table: 'target-table-name'
indices: ['fields', 'to', 'index']
fulltextIndices: ['fields', 'to', 'search', 'in']
```
## Loading JSON data into an InfoGraphDB table:

Data:
```json
[
    {"name": "test", "unit": "PCE"},
    {"name": "foo", "unit": "MTR"},
]
```

Loader:
```yaml
file: 'path/to/file.json'
loader: 'idb-json'
namespace: 'target namespace to import into'
table: 'target-table-name'
indices: ['fields', 'to', 'index']
fulltextIndices: ['fields', 'to', 'search', 'in']
```

## Loading YAML data into an InfoGraphDB table:

Data:
```yaml
name: test
unit: PCE
---
name: foo
unit: MTR
```

Loader:
```yaml
file: 'path/to/file.yml'
loader: 'idb-yaml'
namespace: 'target namespace to import into'
table: 'target-table-name'
indices: ['fields', 'to', 'index']
fulltextIndices: ['fields', 'to', 'search', 'in']
skipUnderscores: false # Determines if keys starting with _ should be ignored.
rowNumber: 'priority' # Specifies an auto-generated field which contains the row-number of each record
```

## Loading InfoGraphDB sets from a YAML file:

Data:
```yaml
set1: [A, B, C]
set2:
```

Loader:
```yaml
file: 'path/to/file.json'
loader: 'idb-yaml-sets'
namespace: 'target namespace to import into'
```