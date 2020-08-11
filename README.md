[![](https://tokei.rs/b1/github/sempare/sempare-streams?category=lines)](https://github.com/sempare/sempare-streams) [![](https://tokei.rs/b1/github/sempare/sempare-streams?category=code)](https://github.com/sempare/sempare-streams) [![](https://tokei.rs/b1/github/sempare/sempare-streams?category=files)](https://github.com/sempare/sempare-streams)

# ![](./images/sempare-logo-45px.png) Sempare Streams

Copyright (c) 2020 [Sempare Limited](http://www.sempare.ltd), [Conrad Vermeulen](mailto:conrad.vermeulen@gmail.com)

Contact: <info@sempare.ltd>

License: [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt)

Open Source: https://github.com/sempare/sempare-streams

# Description

This is a PREVIEW at creating a java streams / linq like interface for Delphi.

# How to use:

```
uses
    Sempare.Streams;
```


The main entry point is the _Stream_ record, where we stream from:
* a list : TList< T >
* a dynamic array : TArray< T >
* a enumerable : TEnumerable< T >
	
There are two special helper functions:
- field(name) 
	used to reference fields in a record/class
- field(name, sortorder)
	used to reference a field and specify sort order (asc/desc) when sorting

# Sample usage

Assume the following:
* need a structure like the following:
``` 
  TAddr = record
    zip: string;
  end;

  TPerson = record
    name: string;
    value: Integer;
    addr: TAddr;
    sugar: boolean;
    num: double;
  end;
```
* people : TList<TPerson>

### To get a single value:
```
var john15 := Stream.From<TPerson>(people).Filter((field('name') = 'john') and (field('value') = 15)).TakeOne;

```

### To count records:
```
var total := Stream.From<TPerson>(Fpeople).Filter((field('num') >= 1.3) and (field('num') <= 1.5)).Count;
```

### Dereferencing fields:
```
var dereferencedFields := Stream.From<TPerson>(Fpeople).Filter((field('addr.zip') = '8800')).Count
```

### Sorting:
```
var Arr := Stream.From<TPerson>(Fpeople).SortBy(field('sugar', asc) and field('name', asc)).ToArray;

```

### Offseting and limiting
```
var arr := Stream.From<TPerson>(Fpeople) //
  .Filter((field('name') = 'john') or (field('num') = 1.2)) //
  .skip(1).take(2).Count; 
```

### Offseting and limiting
```
var arr := Stream.From<TPerson>(Fpeople) //
  .Filter((field('name') = 'john') or (field('num') = 1.2)) //
  .skip(1).take(2).Count; 
```

### Map
```
type
	TName = record
		Name : string;
	end;
	
var arr := Stream.From<TPerson>(Fpeople) //
  .Filter((field('name') = 'john') or (field('num') = 1.2)) //
  .Map<TName>(function(const AValue : TPerson):TName 
  	begin
  		result.name := avalue.name;
  	end)
  .skip(1).take(2).Count; 
```

### Update
```
	
var arr := Stream.From<TPerson>(Fpeople) //
  .Filter((field('name') = 'john') or (field('num') = 1.2)) //
  .skip(1).take(2) //
  .Apply(procedure (const [ref] AValue : TPerson) 
  	begin
  		result.name := avalue.name;
  	end); 
```

# PROTOTYPE NOTES

The query object (TStreamProcessor) is very simplistic, but the objective was to explore how the record operators can be used to provide the required functionality.

# TODO
- GroupBy
  * this should be to a:
  	- TDictionary<TKey, TOutput>, 
  	- TDictionary<TKey, TList<TOutput>>, or 
  	- TDictionary<TKey, TArray<TOutput>>
  * the key and/or value can also be a result of a function
- Allow mapping to simple types - e.g. integer, string
- allow enumerable on simple types - e.g. integer, string
- change design to be a chain of pipeline
- change enumerable datasource to be able to pull a value, or skip a value.
- allow for cached results, to efficiently allow new chains to be based on previous ones without retriggering queries from scratch.
- besides IEnumerable<T>, support an IQueryable<T> interface which would allow for SQL generations and queries...
- support joins: left join, right join, inner join
- rather than returning TDictionary or array, could return json, or allow to be applied to a json stream.
