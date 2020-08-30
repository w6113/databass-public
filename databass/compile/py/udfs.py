from .translator import *

"""
Incremental UDFs are useful in groupbys when building the hashtable

We represent an incremental UDF translator using a dictionary of code snippets.
The code snippets for init and finalize will be inlined, and the code for update 
will be added to the compiler outside of the UDF translator

The dictionary keys are:

  * init        code snippet to initialize aggregate state
  * update      (possibly multi-line) code snippet to update the state
  * finalize    code snippet to finalize 
  * nvars       number of temporary variables to initialize
                defaults to 0

The snippets can use the following notation to reference variables that will 
be passed in:

  {s}    intermediate state
  {v}    value from current row
  {vari} i'th variable, where i rancges from 1 to nvars

"""

class PyUDFTranslatorRegistry(dict):
  _registry = None

  @staticmethod
  def registry():
    if not PyUDFTranslatorRegistry._registry:
      PyUDFTranslatorRegistry._registry = PyUDFTranslatorRegistry()
    return PyUDFTranslatorRegistry._registry
      
pyudftranslatorregistry = PyUDFTranslatorRegistry.registry()
pyudftranslatorregistry["count"] = dict(
    init="0", 
    update="{s} += 1", 
    finalize="{s}"
    )
pyudftranslatorregistry["avg"] = dict(
    init="[0, 0]",
    update="{s}[0] += {v}\n{s}[1] += 1",
    finalize="{s}[0] / {s}[1] if {s}[1] else float('nan')"
    )
pyudftranslatorregistry["sum"] = dict(
    init="0",
    update="{s} += {v}",
    finalize="{s}"
    )
pyudftranslatorregistry["std"] = dict(
    init="[0,0.,0]",
    update="""{s}[0] += 1
{var1} = {v} - {s}[1]
{s}[1] += {var1} / {s}[0]
{s}[2] += {var1} * ({v} - {s}[1])""",
    finalize="float('nan') if {s}[0] < 2 else {s}[2] / ({s}[0] - 1)",
    nvars=1
    )
pyudftranslatorregistry["stddev"] = registry["std"]




