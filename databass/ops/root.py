from ..baseops import *

class Sink(UnaryOp):
  def init_schema(self):
    self.schema = self.c.schema
    return self.schema


class Yield(Sink):
  def __iter__(self):
    return iter(self.c)

class Collect(Sink):
  def __iter__(self):
    return [row for row in self.c]

class Print(Sink):
  def __iter__(self):
    for row in self.c:
      print(row)
    yield 


