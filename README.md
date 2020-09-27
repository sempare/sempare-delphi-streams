[![](https://tokei.rs/b1/github/sempare/sempare-streams?category=lines)](https://github.com/sempare/sempare-streams) [![](https://tokei.rs/b1/github/sempare/sempare-streams?category=code)](https://github.com/sempare/sempare-streams) [![](https://tokei.rs/b1/github/sempare/sempare-streams?category=files)](https://github.com/sempare/sempare-streams)

# ![](./images/sempare-logo-45px.png) Sempare Streams

Copyright (c) 2020 [Sempare Limited](http://www.sempare.ltd), [Conrad Vermeulen](mailto:conrad.vermeulen@gmail.com)

Contact: <info@sempare.ltd>

License: [GPL v3.0](https://www.gnu.org/licenses/gpl-3.0.en.html) or [Sempare Limited Commercial License](./docs/commercial.license.md)

Open Source: https://github.com/sempare/sempare-streams

# Description

The objective is to provide a Java streams / Linq like interface for Delphi leveraging record operators so that query expressions can look quite natural and 
be more functional with less side effects. 

The current implementation is incremental in supporting features and is not focused on performance/optimisation yet.

Features include:
- counting elements
- grouping
- mapping one type to another type
- applying procedures to elements
- sorting
- inner, left, right and full joins


# How to use:

```
uses
    Sempare.Streams;
```


The main entry point is the _Stream_ record, where we stream from:
* a list : <b>TList&lt;T&gt;</b>
* a dynamic array : <b>TArray&lt;T&gt;</b>
* a enumerable : <b>TEnumerable&lt;T&gt;</b> or <b>IEnumerable&lt;T&gt;</b>

# Sample usage

Have a look at some of the [tests](./src/Sempare.Streams.Test.pas).

With the following structure:
``` 
type
  TAddr = record
    id: integer;
    zip: string;
  end;

  TPerson = record
    id: integer;
    name: string;
    Age: integer;
    addrid: integer;
  end;

  TAddrMeta = record
  public
    id: TFieldExpression;
    zip: TFieldExpression;
  end;

  TPersonMeta = record
  public
    [StreamField('name')]
    FirstName: TFieldExpression;
    Age: TFieldExpression;
  end;
  
var
  people : TList<TPerson>;
  Person : TPersonMeta;
  Addr : TPersonMeta;
```

The <b>Stream</b> operations should be able to take place on records, classes and primitive types.

TPersonMeta is a metadata record and should only contain fields of TFieldExpression that map onto the fields in TPerson. Metadata records are used to reference fields in queries. 
The StreamField attribute can be used if an alternative name should be used. The example above illustrates how 'firstname' in the metadata record would map onto 'name' in the TPerson record, 
but it could also be used in the common scenario where properties are F prefixed. e.g. there may be a private 'FName', where the metadata would use 'name'.

### Initialise metadata

This does not do anything complicated. It sets up TFieldExpression using the <b>field</b>() helper methods, so that the the queries can be consistent on specific types.

```
person := Stream.ReflectMetadata<TPersonMeta, TPerson>();
```

Best practice is to define a Meta class for each type of class you are likely to have queries on.

### To get a single value:

Using the 'person' meta, a query becomes quite easy to read.

```
var john15 := Stream.From<TPerson>(people).Filter((person.firstname = 'john') and (person.value = 15)).TakeOne();
```

This will throw an EStreamItemNotFound if an item is not found;

You could also break it up as follows:
```
var expr : TExpression := person.firstname = 'john';
if someConditionIsTrue then
   expr := expr and (person.value = 15);
   
john15 := Stream.From<TPerson>(people).Filter(expr).TakeOne();   
```

### To Array or List

To get values to array or list, use ToArray() or ToList()

```
var johnArr := Stream.From<TPerson>(people).Filter((person.firstname = 'john')).ToArray();
var johnLst := Stream.From<TPerson>(people).Filter((person.firstname = 'john')).ToList();
```

### To count records:
```
var total := Stream.From<TPerson>(Fpeople).Filter((person.number >= 1.3) and (person.number <= 1.5)).Count();
```

### Dereferencing fields:
It may be useful to still use the field() function that can allow for nested fields to be queried easily. 
This is a shortcoming until the Metadata can support nested metadata as well, but will be addressed in future.

```
var dereferencedFields := Stream.From<TPerson>(Fpeople).Filter((field('addr.zip') = '8800')).Count();
```

### Sorting:

Use SortBy() to sort on classes or records. Use Sort() if you want to use traditional ICompare comparators, 
which can work on any type. 

```
// sorting in a single expression
var arr := Stream.From<TPerson>(Fpeople).SortBy(field('sugar', asc) and field('name', asc)).ToArray();


// build up a sort expression
var sortExpr : TSortExpression = field('sugar', asc);
if someConditionIsTrue then
   sortExpr := sortExpr and field('name', asc); 

arr := Stream.From<TPerson>(Fpeople).SortBy(sortExpr).ToArray();
```
### Offseting and limiting

Using skip() and take() you can select how many records you want to receive.

```
var arr := Stream.From<TPerson>(Fpeople) 
  .Filter((person.firstname = 'john') or (person.number = 1.2)) 
  .skip(1).take(2).Count(); 
```

### Map

Map() allows you to apply a function to each of the records from one type to another.

You do need to pay attention to memory leaks that may arise if you are creating classes.

```
type
  TName = record
    Name : string;
  end;
	
var arr := Stream.From<TPerson>(Fpeople)
  .Filter((person.firstname = 'john') or (person.number = 1.2)) 
  .Map<TName>(function(const AValue : TPerson):TName 
  	begin
  		result.name := avalue.name;
  	end)
  .skip(1).take(2).Count(); 
```

### Update

This is how you can apply a procedure to each of the items in the stream.

```
	
var arr := Stream.From<TPerson>(Fpeople) 
  .Map((person.firstname = 'john') or (person.number = 1.2)) 
  .skip(1).take(2) 
  .Apply(procedure (const AValue : TPerson) 
  	begin
  		result.name := 'hello ' + avalue.name;
  	end); 
```

### Joins (inner, left, right and full)

```

var arr := Stream.From<TPerson>(Fpeople)
    .InnerJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs),
    function(const a: TPerson; const b: TAddr): boolean
    begin
      result := a.addrid = b.id;
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;

```

To summarise the joins:

- Perform an inner join on two streams, where items should match in both streams.
- Perform an left join on two streams, where items if first stream are returned, optionally matching items in the second stream.
- Perform an right join on two streams, where items if second stream are returned, optionally matching items in the first stream.
- Performing a full join is a union of left and right joins on the streams.

## Unique

Creates a unique stream of items.

```
  Assert.IsTrue(Stream.From<integer>([1, 2, 3, 4, 5, 7]) //
    .Equals(Stream.From<integer>([5, 4, 2, 7, 3, 3, 2, 7, 1]).Unique));

  Assert.AreEqual(6, Stream.From<integer>([5, 4, 2, 7, 3, 3, 2, 7, 1]).Unique.Count);

```

## Grouping

```
 var  grouping: tdictionary<string, tarray<TPerson>>  := Stream.From<TPerson>(people) //
      .GroupToArray<string>(Person.FirstName);

```

## Optimising your queries  

You should re-arrange your filter(), skip(), take() operations appropriately so that unnecessary map()/apply() actions are not performed.

Using the Cache() you can create a temporary snapshot in order to use other streaming operations
 based on previous transformations.

## Memory management

As mentioned in Map(), you need to remember that you are responsible for any memory allocation during Map() or Apply(), or other function/procedure calls.

The implementation has used Delphi interfaces to free up any resources automatically so you can focus
on your own resources.

## The field() methods

There are three special helper functions:
- <b>field</b>(name) 
	used to reference fields in a record/class
- <b>field</b>(name, sortorder)
	used to reference a field and specify sort order (asc/desc) when sorting
- <b>self</b>()
  used to allow queries to operate on enumerable types of simple types (NOTE: not implemented yet)
  
These are used in queries and by the Meta classes (created by calling Stream.Reflect)

# Restrictions and considerations

* Calling Apply() on records may not work as records are currently referenced by value, so updates don't propagate to the source collection. This will be reviewed
* Calling any methods such as ToArray, TList, Count() use the enumeration to visit all values. 
* Metadata model can only contain fields of TFieldExpression currently. In future, we may support referencing other metadata records.

# License

The Sempare Streams library is dual-licensed. You may choose to use it under the restrictions of the [GPL v3.0](https://www.gnu.org/licenses/gpl-3.0.en.html) at 
no cost to you, or you may purchase for user under the [Sempare Limited Commercial License](./docs/commercial.license.md) 

A commercial licence grants you the right to use Sempare Streams in your own applications, royalty free, and without any requirement to disclose your source code nor any modifications to 
Sempare Streams to any other party. A commercial licence lasts into perpetuity, and entitles you to all future updates, free of charge. 

A commercial licence is sold per developer developing applications that use Sempare Streams. The initial cost is £15 per developer and includes first year of support. 
For support thereafter, a nominal fee of £10 per developer per year if required (the cost of a few cups of coffee). 

Please send an e-mail to info@sempare.ltd to request an invoice which will contain the bank details.

Support and enhancement requests submitted by users that pay for support will be prioritised. New developments may incur additional costs depending on time required for implementation.


# TODO

The roadmap is included in the [TODO list](./docs/todo.md) 