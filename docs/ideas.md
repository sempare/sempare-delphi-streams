# rough ideas for leveraging a data source


# current implementation using an enumerable source
```
var arr := Stream.From<TPerson>(people) //
  .Filter((field('name') = 'john') or (field('num') = 1.2)) //
  .Skip(1).take(2)
  .Select(field('name') and field('age')) // only query name and age
  .ToJsonArray; 
```
# adjustment to allow for IQueryable


so there are 2 approaches with the db I think:
- initially start by just requiring db things to be explicit
- then can also extend to allow for ORM type queries using attribute metadata

## simple query

```  
var arr := Stream.From(connection, 'people', 'p') // p is alias for people
	.LeftJoin('addresses', 'a', field('p.id') = field('a.person_id'))
  .Where(field('p.name') = 'john')
  .Select(field('name') and field('age')) // only query name and age
  .Skip(1).Take(2)
  .ToJsonArray; 
```

## simple query with join
  
## create a prebuilt query using variable(name)

```
var prepared := Stream.From(connection, 'people', 'p')
	.LeftJoin('addresses', 'a', field('p.id') = field('a.person_id'))
  .Where(field('p.name') = variable('name'))
  .Select([field('p.name'), field('a.addr1'), field('p.age')]) // only query name and age
  .Skip(1).take(2);
  
```
  
## use a query
```  
var arr := prepared
	.Using('name', 'john')
	.skip(1).take(1)
	.ToJsonArray();
```
	
# join streams
```
var arr := Stream
	.From(arr, 'p')
	.LeftJoin(arr2, 'a', field('p.id') = field('a.person_id'))
	.Where(field('p.name') = variable('name'))
  .Select([field('p.name'), field('a.addr1'), field('p.age')]) // only query name and age
  .Skip(1).take(2);
```
## into a jsonarray
```
var result := arr.toJsonArray();
```

# to a structure

```
var result := arr.ToArray<TPerson>();

```

# annotations

We may want to annotate the classes. e.g.

this may want to extract into another library, as it is becoming very ORM like...

Use Optional on PrimaryKey... that should be a constraint of the framework. When queried, the primary key is set. When new, primary key is not...

```
type
	[Table('address')]
	TAddr = class
		[PrimaryKey('id'), NotNull] // PrimaryKey implies not null (NotNull not required). Column() not required
 		FId : Optional<integer>;
		
		[ForeignKey('people.id'), Column('person_id'), CascadeUpdate(cuRestrict), CascadeDelete(cuRestrict)]
		FPersonId : integer;
		
		[Column('addr'), NotNull, MaxLength(100), Default('1 Elm Street')] // MinLength implies NotNull
		FAddr : string;
		
	end;
	
  [Table('people')]
  TPerson = class
    [PrimaryKey('id'), NotNull] // PrimaryKey 
  	FId : Optional<integer>;
  	
  	[Column('name'), NotNull, MaxLength(100), Unique]
  	FName : string;
  	
  	[Foreign] // implies transient, details on TAddr
  	FAddr : TAddr;
  	
  	[Transient] // not saved
  	FTransient: byte;	
  	
  	[Foreign] // implies transient, details on TPet
  	FPets : TList<TPet>;
  	
  	[Foreign, ]
  	FFavouriteShops : TList<TShop>;
  	
  end;
  
  [Table('person_shop')]
  TPersonShop = class
  	[PrimaryKey('person_id')]
  	FPerson : integer;
  	
  	[PrimaryKey('shop_id')]
  	FShop : integer;
  end;
  

  [Table('pets')]
  TPet = class
  	[PrimaryKey()] // id is default if nothing specified
  	FId : Optional<integer>;
  	
  	[ForeignKey('people.id'), Column('person_id'), NotNull]
		FPersonId : integer;
  	
  	[Column('name'), Index]
  	FName : string;
  end;
  
  [Table('shops')]
  TShop = class
  	[PrimaryKey]
  	FId : Optional<integer>;
  	
  	[Column('name')]
  	FName : string;
  end;
  
  
```
	var ctx := db.context(); // whatever it is
	var person := TPerson.Create();
	with person do 
	begin
		Name := 'joe';
		Transient := 1;
	end;
	ctx.save(person); 
	 
	person := ctx.query<TPerson>(primarykey = 1).Get(['addr']).TakeOne;
	
	// ---- The streaming interface mapping onto the above query
	person := Stream.From<TPerson>(connection, 'p') // p is alias for people
	   .LeftJoin<TAddr>('a')) // conditions are already on the class
	   .Where(field('p.id') = 1)
	   .TakeOne; 
	// ---- end
	
	
	ctx.delete(person);
	
	var cachedWhere := ctx.cachedWhere<TPerson>(field('name') = variable('name'));

	ctx.delete<TPerson>(field('name') = variable('name'));
	
	var people := ctx.query<TPerson>(cachedWhere.Using('name', 'john')).ToArray<TPerson>();
	
	// ----  The streaming interface mapping onto the above query
	person := Stream.From<TPerson>(connection, 'p') // p is alias for people
	   .Where(field('p.name') = 'john')
	   .TakeOne; 
	// ---- end
	
	var person := TPerson.Create();
	with person do 
	begin
		Name := 'peter';
		Transient := 1;
	end;
	ctx.save(person); // will do insert as id is not set
	person.Name := 'andrew'; 
	ctx.save(person); // will do update as id is set
	

```

things to consider - memory management
- once an object is queried, it can be 'cached' so that changes are tracked to optimise updates
- once an object is saved, it is tracked



- would need an adapter to translate results for visual binding or mapping into datasets
