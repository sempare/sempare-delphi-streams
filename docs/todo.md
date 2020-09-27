
# TODO

- support GroupBy keys being the result of a function
- improve caching support
- support using field expressions in join statements
- support IQueryable<T> interface which would allow for SQL generations and queries. See [ideas](./ideas.md)
- support ToJson as a result
- support nested metadata
- support early type checking (e.g. when stream.reflect() is used, metadata can store type information, which can then be used to validate comparison operations, etc)
- review performance and size (generics are used to preserve type)
- benchmark performance so cost of features are known