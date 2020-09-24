
# TODO

- support GroupBy
  * this should be to a:
  	- TDictionary<TKey, TOutput>, 
  	- TDictionary<TKey, TList<TOutput>>, or 
  	- TDictionary<TKey, TArray<TOutput>>
  * the key and/or value can also be a result of a function
- support caching
- support IQueryable<T> interface which would allow for SQL generations and queries. See [ideas](./ideas.md)
- support joins: left join, right join, inner join
- support ToJson as a result
- support nested metadata
- support early type checking (e.g. when stream.reflect() is used, metadata can store type information, which can then be used to validate comparison operations, etc)
- review performance and size (generics are used to preserve type)
- benchmark performance so cost of features are known