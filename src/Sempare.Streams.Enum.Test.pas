(*%****************************************************************************
 *                 ___                                                        *
 *                / __|  ___   _ __    _ __   __ _   _ _   ___                *
 *                \__ \ / -_) | '  \  | '_ \ / _` | | '_| / -_)               *
 *                |___/ \___| |_|_|_| | .__/ \__,_| |_|   \___|               *
 *                                    |_|                                     *
 ******************************************************************************
 *                                                                            *
 *                        Sempare Streams                                     *
 *                                                                            *
 *                                                                            *
 *          https://www.github.com/sempare/sempare-streams                    *
 ******************************************************************************
 *                                                                            *
 * Copyright (c) 2020 Sempare Limited,                                        *
 *                    Conrad Vermeulen <conrad.vermeulen@gmail.com>           *
 *                                                                            *
 * Contact: info@sempare.ltd                                                  *
 *                                                                            *
 * Licensed under the GPL Version 3.0 or the Sempare Commercial License       *
 * You may not use this file except in compliance with one of these Licenses. *
 * You may obtain a copy of the Licenses at                                   *
 *                                                                            *
 * https://www.gnu.org/licenses/gpl-3.0.en.html                               *
 * https://github.com/sempare/sempare-streams/tree/dev/docs/commercial.license.md *
 *                                                                            *
 * Unless required by applicable law or agreed to in writing, software        *
 * distributed under the Licenses is distributed on an "AS IS" BASIS,          *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 * See the License for the specific language governing permissions and        *
 * limitations under the License.                                             *
 *                                                                            *
 ****************************************************************************%*)
unit Sempare.Streams.Enum.Test;

interface

uses
  Sempare.Streams,
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams.Enum,
  Sempare.Streams.Test.Common,
  DUnitX.TestFramework;

type

  [TestFixture]
  TStreamEnumTest = class(TStreamsTestBase)
  public
    [Setup]
    procedure Setup; override;

    [Teardown]
    procedure Teardown; override;

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
    procedure TestDataSetEnum;

    [Test]
    procedure TestDataSetEnumRecord;
  end;

type
  TPersonDataSet = class
  public
    [StreamField('name')]
    FName: string;
    [StreamField('age')]
    fage: integer;
    [StreamField('weight')]
    fweight: double;
    [StreamField('dob')]
    fdob: tdatetime;
  end;

  TPersonDataSetMeta = record [StreamField('fname')]
    Name: TFieldExpression;
    [StreamField('fage')]
    Age: TFieldExpression;
    [StreamField('fweight')]
    Weight: TFieldExpression;
    [StreamField('fdob')]
    DoB: TFieldExpression;
  end;

  TPersonDataSetRecord = record
  public
    Name: string;
    Age: integer;
    Weight: double;
    DoB: tdatetime;
  end;

  TPersonDataSetMetaRecord = record
    Name: TFieldExpression;
    Age: TFieldExpression;
    Weight: TFieldExpression;
    DoB: TFieldExpression;
  end;

implementation

uses
  Data.DB,
  FireDAC.Comp.Client,
  System.classes,

  Sempare.Streams.Types;

{ TStreamEnumTest }

type
  TInt = class
  public
    value: integer;
    constructor Create(const avalue: integer);
  end;

procedure TStreamEnumTest.Setup;
begin
  inherited;
end;

procedure TStreamEnumTest.Teardown;
begin
  inherited;
end;

procedure TStreamEnumTest.TestApplyEnum;
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

procedure TStreamEnumTest.TestArrayEnum;
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

function CreateMockUsersTable(): TFDMemTable;
var
  ds: TFDMemTable;
begin
  ds := TFDMemTable.Create(nil);
  with ds do
  begin
    FieldDefs.Add('name', ftWideString, 20);
    FieldDefs.Add('age', ftInteger);
    FieldDefs.Add('weight', ftFloat);
    FieldDefs.Add('dob', ftDateTime);
    CreateDataSet;
  end;
  with ds do
  begin
    Append;
    FieldByName('name').value := 'joe';
    FieldByName('age').value := 5;
    FieldByName('weight').value := 15;
    FieldByName('dob').value := EncodeDate(2015, 6, 30);
    Post;
    Append;
    FieldByName('name').value := 'pete';
    FieldByName('age').value := 6;
    FieldByName('weight').value := 20;
    FieldByName('dob').value := EncodeDate(2014, 5, 30);
    Post;
    Append;
    FieldByName('name').value := 'jane';
    FieldByName('age').value := 7;
    FieldByName('weight').value := 25;
    FieldByName('dob').value := EncodeDate(2013, 5, 30);
    Post;
  end;
  result := ds;
end;

procedure TStreamEnumTest.TestDataSetEnum;
var
  ds: TFDMemTable;
  person: TPersonDataSetMeta;
  result: TArray<TPersonDataSet>;
  p: TPersonDataSet;
begin
  ds := CreateMockUsersTable();
  try
    person := stream.ReflectMetadata<TPersonDataSetMeta, TPersonDataSet>;
    result := stream.From<TPersonDataSet>(ds).ToArray();

    Assert.AreEqual('joe', result[0].FName);
    Assert.AreEqual('pete', result[1].FName);
    Assert.AreEqual('jane', result[2].FName);

    for p in result do
      p.Free;
    result := nil;
  finally
    ds.Free;
  end;
end;

procedure TStreamEnumTest.TestDataSetEnumRecord;
var
  ds: TFDMemTable;
  person: TPersonDataSetMetaRecord;
  result: TArray<TPersonDataSetRecord>;
begin
  ds := CreateMockUsersTable();
  try
    person := stream.ReflectMetadata<TPersonDataSetMetaRecord, TPersonDataSetRecord>;
    result := stream.From<TPersonDataSetRecord>(ds).ToArray();

    Assert.AreEqual('joe', result[0].Name);
    Assert.AreEqual('pete', result[1].Name);
    Assert.AreEqual('jane', result[2].Name);

    result := nil;
  finally
    ds.Free;
  end;
end;

procedure TStreamEnumTest.TestEnumerableEnum;
var
  a: TList<integer>;
  e: IEnum<integer>;
  l: TList<integer>;
begin
  a := TList<integer>.Create();
  a.AddRange([1, 2, 3]);
  l := TList<integer>.Create();
  try
    e := TTEnumerableEnum<integer>.Create(a);
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

procedure TStreamEnumTest.TestFilterEnum;
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
      e := TfilterEnum<integer>.Create(TTEnumerableEnum<integer>.Create(a),
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
      e := TfilterEnum<integer>.Create(TTEnumerableEnum<integer>.Create(a),
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

procedure TStreamEnumTest.TestMapEnum;
var
  a: TList<integer>;
  e: IEnum<string>;
  l: TList<string>;
begin
  a := TList<integer>.Create();
  a.AddRange([1, 2, 3]);
  l := TList<string>.Create();
  try
    e := TMapEnum<integer, string>.Create(TTEnumerableEnum<integer>.Create(a),
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

procedure TStreamEnumTest.TestSkipEnum;
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

procedure TStreamEnumTest.TestTakeEnum;
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

{ TInt }

constructor TInt.Create(const avalue: integer);
begin
  value := avalue;
end;

initialization

TDUnitX.RegisterTestFixture(TStreamEnumTest);

end.
