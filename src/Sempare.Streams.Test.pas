unit Sempare.Streams.Test;

interface

uses
  System.Generics.Collections,
  DUnitX.TestFramework;

type
  TAddr = record
    zip: string;
  end;

  TPerson = record
    name: string;
    value: Integer;
    addr: TAddr;
    sugar: boolean;
    num: double;
    constructor Create(const AName: string; avalue: Integer; const APostCode: string; asugar: boolean; anum: double);
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
    [Test, Ignore]
    procedure TestFilterNestedRecord;
    [Test]
    procedure TestFilterInteger;
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
    procedure TestFilterSimpleSortInteger;
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

procedure TMyTestObject.TestFilterAnd;
begin
  fperson := Stream.From<TPerson>(Fpeople).Filter((field('name') = 'john') and (field('value') = 15)).TakeOne;
  assert.AreEqual('john', fperson.name);
end;

procedure TMyTestObject.TestFilterBoolean;
begin
  assert.AreEqual(3, Stream.From<TPerson>(Fpeople).Filter((field('sugar') = true)).Count);
  assert.AreEqual(4, Stream.From<TPerson>(Fpeople).Filter((field('sugar') = false)).Count);
end;

procedure TMyTestObject.TestFilterDouble;
begin
  assert.AreEqual(3, Stream.From<TPerson>(Fpeople).Filter((field('num') >= 1.3) and (field('num') <= 1.5)).Count);
end;

procedure TMyTestObject.TestFilterInteger;
begin
  assert.AreEqual(2, Stream.From<TPerson>(Fpeople).Filter((field('value') = 16)).Count);
end;

procedure TMyTestObject.TestFilterNestedRecord;
begin
  assert.AreEqual(2, Stream.From<TPerson>(Fpeople).Filter((field('addr.zip') = '8800')).Count);

end;

procedure TMyTestObject.TestFilterNot;
begin
  assert.AreEqual(5, Stream.From<TPerson>(Fpeople).Filter(not(field('value') = 16)).Count);
end;

procedure TMyTestObject.TestFilterOr;
begin
  assert.AreEqual(4, Stream.From<TPerson>(Fpeople).Filter((field('name') = 'john') or (field('num') = 1.2)).Count);

end;

procedure TMyTestObject.TestFilterSimpleSortBoolean;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('sugar', asc) and field('name', asc)).ToArray;
  assert.isfalse(FArr[0].sugar);
  assert.AreEqual('john', FArr[0].name);
  assert.istrue(FArr[high(FArr)].sugar);
  assert.AreEqual('matthew', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('sugar', desc) and field('name', asc)).ToArray;
  assert.istrue(FArr[0].sugar);
  assert.AreEqual('abraham', FArr[0].name);
  assert.isfalse(FArr[high(FArr)].sugar);
  assert.AreEqual('peter', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSimpleSortDouble;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('num', asc) and field('name', asc)).ToArray;
  assert.isfalse(FArr[0].sugar);
  assert.AreEqual('john', FArr[0].name);
  assert.istrue(FArr[high(FArr)].sugar);
  assert.AreEqual('grant', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('num', desc) and field('name', asc)).ToArray;
  assert.istrue(FArr[0].sugar);
  assert.AreEqual('grant', FArr[0].name);
  assert.isfalse(FArr[high(FArr)].sugar);
  assert.AreEqual('john', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSimpleSortInteger;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('value', asc) and field('name', asc)).ToArray;
  assert.isfalse(FArr[0].sugar);
  assert.AreEqual('peter', FArr[0].name);
  assert.isfalse(FArr[high(FArr)].sugar);
  assert.AreEqual('john', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('value', desc) and field('name', asc)).ToArray;
  assert.isfalse(FArr[0].sugar);
  assert.AreEqual('john', FArr[0].name);
  assert.isfalse(FArr[high(FArr)].sugar);
  assert.AreEqual('peter', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSimpleSortString;
begin
  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('name', asc)).ToArray;
  assert.istrue(FArr[0].sugar);
  assert.AreEqual('abraham', FArr[0].name);
  assert.isfalse(FArr[high(FArr)].sugar);
  assert.AreEqual('peter', FArr[high(FArr)].name);

  FArr := Stream.From<TPerson>(Fpeople).SortBy(field('name', desc)).ToArray;
  assert.isfalse(FArr[0].sugar);
  assert.AreEqual('peter', FArr[0].name);
  assert.istrue(FArr[high(FArr)].sugar);
  assert.AreEqual('abraham', FArr[high(FArr)].name);
end;

procedure TMyTestObject.TestFilterSkipAndTake;
begin
  assert.AreEqual(3, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(1).Count);
  assert.AreEqual(2, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(2).Count);

  assert.AreEqual(2, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(1).take(2).Count);

  assert.AreEqual(1, Stream.From<TPerson>(Fpeople) //
    .Filter((field('name') = 'john') or (field('num') = 1.2)) //
    .skip(1).take(1).Count);

end;

{ TPerson }

constructor TPerson.Create(const AName: string; avalue: Integer; const APostCode: string; asugar: boolean; anum: double);
begin
  name := AName;
  value := avalue;
  addr.zip := APostCode;
  sugar := asugar;
  num := anum;
end;

initialization

TDUnitX.RegisterTestFixture(TMyTestObject);

end.
