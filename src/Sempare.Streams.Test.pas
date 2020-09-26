unit Sempare.Streams.Test;

interface

uses
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams.Enum,
  DUnitX.TestFramework;

type
  TAddr = record
    id: integer;
    zip: string;
    constructor Create(const aid: integer; const azip: string);
  end;

  TPerson = record
    id: integer;
    name: string;
    value: integer;
    addr: TAddr;
    sugar: boolean;
    num: double;
    addrid: integer;
    constructor Create(const aid: integer; const AName: string; avalue: integer; const APostCode: string; asugar: boolean; anum: double; aaddrid: integer);
  end;

  [TestFixture]
  TMyTestObject = class(TObject)
  private
    Fpeople: TList<TPerson>;
    Faddrs: TList<TAddr>;
    Faddrs2: TList<TAddr>;
    FArr: TArray<TPerson>;
    fperson: TPerson;
    function CreatePeople: TList<TPerson>;
    function Createaddrs: TList<TAddr>;
    function Createaddrs2: TList<TAddr>;

  public
    [Setup]
    procedure Setup;

    [Teardown]
    procedure Teardown;

    [Test]
    procedure TestArrayEnum;

    [Test]
    procedure TestEnumerableEnum;

    [Test]
    procedure TestSkipEnum;

    [Test]
    procedure TestTakeEnum;

    [Test]
    procedure TestMapEnum;

    [Test]
    procedure TestFilterEnum;

    [Test]
    procedure TestApplyEnum;

    [Test]
    procedure TestFieldExpr;

    [Test, Ignore]
    procedure TestFilterNestedRecord;

    [Test]
    procedure TestFilterinteger;

    [Test]
    procedure TestFilterDouble;

    [Test]
    procedure TestFilterBoolean;

    [Test]
    procedure TestFilterAnd;

    [Test]
    procedure TestFilterOr;

    [Test]
    procedure TestFilterNot;

    [Test]
    procedure TestFilterSimpleSortString;

    [Test]
    procedure TestFilterSimpleSortinteger;

    [Test]
    procedure TestFilterSimpleSortDouble;

    [Test]
    procedure TestFilterSimpleSortBoolean;

    [Test]
    procedure TestFilterSkipAndTake;

    [Test]
    procedure TestJoin;

    [Test]
    procedure TestLeftJoin;

    [Test]
    procedure TestRightJoin;

    [Test, Ignore] // TODO: validate
    procedure TestFullJoin;

  end;

implementation

uses
  System.classes,
  Sempare.Streams,
  Sempare.Streams.Types;

{ TMyTestObject }

function TMyTestObject.Createaddrs: TList<TAddr>;
begin
  result := TList<TAddr>.Create;
  with result do
  begin
    Add(TAddr.Create(1, 'addr1'));
    Add(TAddr.Create(2, 'addr2'));
    Add(TAddr.Create(3, 'addr3'));
    Add(TAddr.Create(4, 'addr4'));
    Add(TAddr.Create(5, 'addr5'));
  end;
end;

function TMyTestObject.Createaddrs2: TList<TAddr>;
begin
  result := TList<TAddr>.Create;
  with result do
  begin
    Add(TAddr.Create(0, 'addr0'));
    Add(TAddr.Create(2, 'addr2'));
    Add(TAddr.Create(7, 'addr7'));
    Add(TAddr.Create(8, 'addr8'));
  end;

end;

function TMyTestObject.CreatePeople: TList<TPerson>;
begin
  result := TList<TPerson>.Create;
  with result do
  begin
    Add(TPerson.Create(1, 'peter', 10, '7700', false, 1.2, 0));
    Add(TPerson.Create(2, 'john', 15, '7705', false, 1.3, 1));
    Add(TPerson.Create(3, 'mary', 14, '7800', false, 1.1, 2));
    Add(TPerson.Create(4, 'matthew', 16, '8800', true, 1.2, 0));
    Add(TPerson.Create(5, 'abraham', 12, '8800', true, 1.3, 4));
    Add(TPerson.Create(6, 'grant', 16, '8845', true, 1.5, 4));
    Add(TPerson.Create(7, 'john', 17, '7805', false, 1.0, 0));
  end;
end;

procedure TMyTestObject.Setup;
begin
  Fpeople := CreatePeople;
  Faddrs := Createaddrs;
  Faddrs2 := Createaddrs2;
end;

procedure TMyTestObject.Teardown;
begin
  Fpeople.Free;
  Faddrs.Free;
  Faddrs2.Free;
end;

type
  TInt = class
  public
    value: integer;
    constructor Create(const avalue: integer);
  end;

procedure TMyTestObject.TestApplyEnum;
var
  a: TArray<integer>;
  e: IEnum<integer>;
  l: TList<integer>;
begin
  a := [1, 2, 3, 4, 5];
  l := TList<integer>.Create();
  try

    e := TMapEnum<TInt, integer>.Create( //

      TApplyEnum<TInt>.Create( //

      TfilterEnum<TInt>.Create(

      TMapEnum<integer, TInt>.Create( //

      TArrayEnum<integer>.Create(a),

      function(const avalue: integer): TInt
      begin
        result := TInt.Create(avalue);
      end), // TMapEnum
      function(const avalue: TInt): boolean
      begin
        result := avalue.value mod 2 = 0;
        if not result then
          avalue.Free;
      end), // TFilterEnum

      procedure(var avalue: TInt)
      begin
        avalue.value := avalue.value + 10;
      end), // TApplyEnum

      function(const avalue: TInt): integer
      begin
        result := avalue.value;
        avalue.Free;
      end);

    while e.HasMore do
      l.Add(e.Current);
    Assert.AreEqual(2, l.Count);
    Assert.AreEqual(12, l[0]);
    Assert.AreEqual(14, l[1]);
  finally
    l.Free;
  end;
end;

procedure TMyTestObject.TestArrayEnum;
var
  a: TArray<integer>;
  e: IEnum<integer>;
  l: TList<integer>;
  i: integer;
begin
  a := [1, 2, 3];
  l := TList<integer>.Create();
  try
    e := TArrayEnum<integer>.Create(a);
    while e.HasMore do
      l.Add(e.Current);
    for i := 0 to high(a) do
      Assert.AreEqual(a[i], l[i]);
  finally
    l.Free;
  end;
end;

procedure TMyTestObject.TestEnumerableEnum;
var
  a: TList<integer>;
  e: IEnum<integer>;
  l: TList<integer>;
begin
  a := TList<integer>.Create();
  a.AddRange([1, 2, 3]);
  l := TList<integer>.Create();
  try
    e := TEnumerableEnum2<integer>.Create(a);
    while e.HasMore do
      l.Add(e.Current);
    Assert.AreEqual(a[0], l[0]);
    Assert.AreEqual(a[1], l[1]);
    Assert.AreEqual(a[2], l[2]);
  finally
    a.Free;
    l.Free;
  end;
end;

type
  TPersonContext = record
    name: TFieldExpression;
    value: TFieldExpression;
    addr: TFieldExpression;
    sugar: TFieldExpression;
    num: TFieldExpression;
  end;

procedure TMyTestObject.TestFieldExpr;
var
  Person: TPersonContext;
begin
  Person := Stream.ReflectMetadata<TPersonContext, TPerson>;
  fperson := Stream.From<TPerson>(Fpeople) //
    .Filter((Person.name = 'john') and (Person.value = 15)).TakeOne;
  Assert.AreEqual('john', fperson.name);
end;

procedure TMyTestObject.TestFilterAnd;
begin
  fperson := Stream.From<TPerson>(Fpeople).Filter((field('name') = 'john') and (field('value') = 15)).TakeOne;
  Assert.AreEqual('john', fperson.name);
end;

procedure TMyTestObject.TestFilterBoolean;
begin
  Assert.AreEqual(3, Stream.From<TPerson>(Fpeople).Filter((field('sugar') = true)).Count);
  Assert.AreEqual(4, Stream.From<TPerson>(Fpeople).Filter((field('sugar') = false)).Count);
end;

procedure TMyTestObject.TestFilterDouble;
begin
  Assert.AreEqual(3, Stream.From<TPerson>(Fpeople).Filter((field('num') >= 1.3) and (field('num') <= 1.5)).Count);
end;

procedure TMyTestObject.TestFilterEnum;
var
  a: TList<integer>;
  e: IEnum<integer>;
  l: TList<integer>;
begin
  a := TList<integer>.Create();
  a.AddRange([1, 2, 3, 4, 5]);
  try
    l := TList<integer>.Create();
    try
      e := TfilterEnum<integer>.Create(TEnumerableEnum2<integer>.Create(a),
        function(const avalue: integer): boolean
        begin
          result := avalue mod 2 = 0
        end);
      while e.HasMore do
        l.Add(e.Current);
      Assert.AreEqual(2, l.Count);
      Assert.AreEqual(2, l[0]);
      Assert.AreEqual(4, l[1]);
    finally
      l.Free;
    end;
    l := TList<integer>.Create();
    try
      e := TfilterEnum<integer>.Create(TEnumerableEnum2<integer>.Create(a),
        function(const avalue: integer): boolean
        begin
          result := avalue mod 2 = 1
        end);
      while e.HasMore do
        l.Add(e.Current);
      Assert.AreEqual(3, l.Count);
      Assert.AreEqual(1, l[0]);
      Assert.AreEqual(3, l[1]);
      Assert.AreEqual(5, l[2]);
    finally
      l.Free;
    end;

  finally
    a.Free;
  end;
end;

procedure TMyTestObject.TestFilterinteger;
begin
  Assert.AreEqual(2, Stream.From<TPerson>(Fpeople).Filter((field('value') = 16)).Count);
end;

procedure TMyTestObject.TestFilterNestedRecord;
begin
  Assert.AreEqual(2, Stream.From<TPerson>(Fpeople).Filter((field('addr.zip') = '8800')).Count);

end;

procedure TMyTestObject.TestFilterNot;
begin
  Assert.AreEqual(5, Stream.From<TPerson>(Fpeople).Filter(not(field('value') = 16)).Count);
end;

procedure TMyTestObject.TestFilterOr;
begin
  Assert.AreEqual(4, Stream.From<TPerson>(Fpeople).Filter((field('name') = 'john') or (field('num') = 1.2)).Count);

end;

procedure TMyTestObject.TestFilterSimpleSortBoolean;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('sugar', asc) and field('name', asc)).ToArray;
  Assert.isfalse(FArr[0].sugar);
  Assert.AreEqual('john', FArr[0].name);
  Assert.istrue(FArr[high(FArr)].sugar);
  Assert.AreEqual('matthew', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('sugar', desc) and field('name', asc)).ToArray;
  Assert.istrue(FArr[0].sugar);
  Assert.AreEqual('abraham', FArr[0].name);
  Assert.isfalse(FArr[high(FArr)].sugar);
  Assert.AreEqual('peter', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSimpleSortDouble;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('num', asc) and field('name', asc)).ToArray;
  Assert.isfalse(FArr[0].sugar);
  Assert.AreEqual('john', FArr[0].name);
  Assert.istrue(FArr[high(FArr)].sugar);
  Assert.AreEqual('grant', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('num', desc) and field('name', asc)).ToArray;
  Assert.istrue(FArr[0].sugar);
  Assert.AreEqual('grant', FArr[0].name);
  Assert.isfalse(FArr[high(FArr)].sugar);
  Assert.AreEqual('john', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSimpleSortinteger;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('value', asc) and field('name', asc)).ToArray;
  Assert.isfalse(FArr[0].sugar);
  Assert.AreEqual('peter', FArr[0].name);
  Assert.isfalse(FArr[high(FArr)].sugar);
  Assert.AreEqual('john', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('value', desc) and field('name', asc)).ToArray;
  Assert.isfalse(FArr[0].sugar);
  Assert.AreEqual('john', FArr[0].name);
  Assert.isfalse(FArr[high(FArr)].sugar);
  Assert.AreEqual('peter', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSimpleSortString;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('name', asc)).ToArray;
  Assert.istrue(FArr[0].sugar);
  Assert.AreEqual('abraham', FArr[0].name);
  Assert.isfalse(FArr[high(FArr)].sugar);
  Assert.AreEqual('peter', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('name', desc)).ToArray;
  Assert.isfalse(FArr[0].sugar);
  Assert.AreEqual('peter', FArr[0].name);
  Assert.istrue(FArr[high(FArr)].sugar);
  Assert.AreEqual('abraham', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSkipAndTake;
begin
  Assert.AreEqual(3, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(1).Count);
  Assert.AreEqual(2, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(2).Count);

  Assert.AreEqual(2, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(1).take(2).Count);

  Assert.AreEqual(1, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(1).take(1).Count);

end;

type
  TJoinedPersons = record
    Person: TPerson;
    addr: TAddr;
  end;

procedure TMyTestObject.TestFullJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .FullJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs2), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      result := a.addrid = b.id;
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(6, length(res));
end;

procedure TMyTestObject.TestJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .InnerJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      result := a.addrid = b.id;
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(4, length(res));
  Assert.AreEqual('john', res[0].Person.name);
  Assert.AreEqual(1, res[0].addr.id);
  Assert.AreEqual('mary', res[1].Person.name);
  Assert.AreEqual(2, res[1].addr.id);
  Assert.AreEqual('abraham', res[2].Person.name);
  Assert.AreEqual(4, res[2].addr.id);
  Assert.AreEqual('grant', res[3].Person.name);
  Assert.AreEqual(4, res[3].addr.id);
end;

procedure TMyTestObject.TestLeftJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .LeftJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      result := a.addrid = b.id;
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(7, length(res));

  Assert.AreEqual('peter', res[0].Person.name);
  Assert.AreEqual(0, res[0].addr.id);

  Assert.AreEqual('john', res[1].Person.name);
  Assert.AreEqual(1, res[1].addr.id);

  Assert.AreEqual('mary', res[2].Person.name);
  Assert.AreEqual(2, res[2].addr.id);

  Assert.AreEqual('matthew', res[3].Person.name);
  Assert.AreEqual(0, res[3].addr.id);

  Assert.AreEqual('abraham', res[4].Person.name);
  Assert.AreEqual(4, res[4].addr.id);

  Assert.AreEqual('grant', res[5].Person.name);
  Assert.AreEqual(4, res[5].addr.id);

  Assert.AreEqual('john', res[6].Person.name);
  Assert.AreEqual(0, res[6].addr.id);

end;

procedure TMyTestObject.TestMapEnum;
var
  a: TList<integer>;
  e: IEnum<string>;
  l: TList<string>;
begin
  a := TList<integer>.Create();
  a.AddRange([1, 2, 3]);
  l := TList<string>.Create();
  try
    e := TMapEnum<integer, string>.Create(TEnumerableEnum2<integer>.Create(a),
      function(const avalue: integer): string
      begin
        result := inttostr(avalue);
      end);
    while e.HasMore do
      l.Add(e.Current);
    Assert.AreEqual(inttostr(a[0]), l[0]);
    Assert.AreEqual(inttostr(a[1]), l[1]);
    Assert.AreEqual(inttostr(a[2]), l[2]);
  finally
    a.Free;
    l.Free;
  end;
end;

procedure TMyTestObject.TestRightJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .RightJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs2), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      result := a.addrid = b.id;
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(6, length(res));

  Assert.AreEqual('peter', res[0].Person.name);
  Assert.AreEqual(0, res[0].addr.id);

  Assert.AreEqual('matthew', res[1].Person.name);
  Assert.AreEqual(0, res[1].addr.id);

  Assert.AreEqual('john', res[2].Person.name);
  Assert.AreEqual(0, res[2].addr.id);

  Assert.AreEqual('mary', res[3].Person.name);
  Assert.AreEqual(2, res[3].addr.id);

  Assert.AreEqual('', res[4].Person.name);
  Assert.AreEqual(7, res[4].addr.id);

  Assert.AreEqual('', res[5].Person.name);
  Assert.AreEqual(8, res[5].addr.id);

end;

procedure TMyTestObject.TestSkipEnum;
var
  a: TArray<integer>;
  e: IEnum<integer>;
  l: TList<integer>;
begin
  a := [1, 2, 3, 4, 5];
  l := TList<integer>.Create();
  try
    e := TSkip<integer>.Create(TArrayEnum<integer>.Create(a), 3);
    while e.HasMore do
      l.Add(e.Current);
    Assert.AreEqual(2, l.Count);
    Assert.AreEqual(4, l[0]);
    Assert.AreEqual(5, l[1]);
  finally
    l.Free;
  end;
end;

procedure TMyTestObject.TestTakeEnum;
var
  a: TArray<integer>;
  e: IEnum<integer>;
  l: TList<integer>;
begin
  a := [1, 2, 3, 4, 5];
  l := TList<integer>.Create();
  try
    e := TTake<integer>.Create(TArrayEnum<integer>.Create(a), 2);
    while e.HasMore do
      l.Add(e.Current);
    Assert.AreEqual(2, l.Count);
    Assert.AreEqual(1, l[0]);
    Assert.AreEqual(2, l[1]);
  finally
    l.Free;
  end;
end;

{ TPerson }

constructor TPerson.Create(const aid: integer; const AName: string; avalue: integer; const APostCode: string; asugar: boolean; anum: double; aaddrid: integer);
begin
  id := aid;
  name := AName;
  value := avalue;
  addr.zip := APostCode;
  sugar := asugar;
  num := anum;
  addrid := aaddrid;
end;

{ TInt }

constructor TInt.Create(const avalue: integer);
begin
  value := avalue;
end;

{ TAddr }

constructor TAddr.Create(const aid: integer; const azip: string);
begin
  id := aid;
  zip := azip;
end;

initialization

TDUnitX.RegisterTestFixture(TMyTestObject);

end.
