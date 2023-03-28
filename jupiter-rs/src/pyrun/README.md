# PyRun Kernels

Permits to define kernels using the loader `pyrun-kernel`. The target file to 
load is a ZIP archive which contains all required resources as well as a
`kernel.json` file.

A rough outline of such a file would be:
````python
VERSION = "v1.2.3"

# (do heavy-lifting here...)...
# use print(..) to signal the progress of the startup sequence...

def kernel(json):
     return { result: json["input"] * 2 }
````

The json input and output can be freely defined. A kernel can be invoked using
`PY.RUN <KERNEL> <JSON-STRING>` and will yield a single string as response.

Using `PY.STATS` an overview of all loaded kernels can be retrieved.

A loader example could be:
````yaml
file: "/kernel.zip"
namespace: "core"
loader: "pyrun-kernel"
name: "my-lovely-kernel"
num_kernels: 1
````
