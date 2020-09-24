unit Sempare.Streams.Test;

interface

uses
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams.Enum,
  DUnitX.TestFramework;

type
  TAddr = record
    zip: string;
  end;

  TPerson = record
    name: string;
    value: integer;
    addr: TAddr;
    sugar: boolean;
    num: double;
    constructor Create(const AName: string; avalue: integer; const APostCode: string; asugar: boolean; anum: double);
  end;

  [TestFixture]
  TMyTestObject = class(TObject)
  private
    Fpeople: TList<TPerson>;
    FArr: TArray<TPerson>;
    fperson: TPerson;
    function CreatePeople: TList<TPerson>;

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
  end;

implementation

uses
  System.classes,
  Sempare.Streams;

{ TMyTestObject }

function TMyTestObject.CreatePeople: TList<TPerson>;
begin
  result := TList<TPerson>.Create;
  with result do
  begin
    Add(TPerson.Create('peter', 10, '7700', false, 1.2));
    Add(TPerson.Create('john', 15, '7705', false, 1.3));
    Add(TPerson.Create('mary', 14, '7800', false, 1.1));
    Add(TPerson.Create('matthew', 16, '8800', true, 1.2));
    Add(TPerson.Create('abraham', 12, '8800', true, 1.3));
    Add(TPerson.Create('grant', 16, '8845', true, 1.5));
    Add(TPerson.Create('john', 17, '7805', false, 1.0));
  end;
end;

procedure TMyTestObject.Setup;
begin
  Fpeople := CreatePeople;
end;

procedure TMyTestObject.Teardown;
begin
  Fpeople.Free;
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

constructor TPerson.Create(const AName: string; avalue: integer; const APostCode: string; asugar: boolean; anum: double);
begin
  name := AName;
  value := avalue;
  addr.zip := APostCode;
  sugar := asugar;
  num := anum;
end;

{ TInt }

constructor TInt.Create(const avalue: integer);
begin
  value := avalue;
end;

initialization

TDUnitX.RegisterTestFixture(TMyTestObject);

end.
