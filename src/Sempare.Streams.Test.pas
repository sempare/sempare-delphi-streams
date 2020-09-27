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
unit Sempare.Streams.Test;

// tests in this file should be standalone and not reliant on setup and teardown methods.
// the idea is that these examples can be easily inspected as a type of 'documentation'

interface

uses
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams,
  Sempare.Streams.Test.Common,
  DUnitX.TestFramework;

type
  TAddr = record
    id: integer;
    zip: string;
    constructor Create(const aid: integer; const azip: string);
  end;

  TAddrMeta = record
  public
    id: TFieldExpression;
    zip: TFieldExpression;
  end;

  TPerson = record
    id: integer;
    name: string;
    Age: integer;
    addrid: integer;
    constructor Create(const aid: integer; const aname: string; const aage: integer; const aaddrid: integer);
  end;

  TPersonMeta = record
  public
    [StreamField('name')]
    FirstName: TFieldExpression;
    Age: TFieldExpression;
  end;

  TPersonAddr = record
    id: integer;
    FirstName: string;
    Age: string;

    addr: record
      zip: string;
    end;
  end;

  [TestFixture]
  TStreamTest = class(TStreamsTestBase)
  public

    [Test]
    procedure TestTakeOneFound;

    [Test, WillRaise(EStreamItemNotFound)]
    procedure TestTakeOneNotFound;

    [Test]
    procedure TestCount;

    [Test]
    procedure TestToArrayAndList;

    [Test]
    procedure TestSort;

    [Test]
    procedure TestUnique;

    [Test]
    procedure TestGroupBy;

  end;

implementation

{ TStreamTest }

procedure TStreamTest.TestCount;
var
  people: TList<TPerson>;
  Person: TPersonMeta;
begin
  people := TList<TPerson>.Create;
  try
    people.Add(TPerson.Create(1, 'peter', 10, 0));
    people.Add(TPerson.Create(2, 'john', 15, 0));
    people.Add(TPerson.Create(3, 'mary', 8, 0));
    Person := Stream.ReflectMetadata<TPersonMeta, TPerson>();
    Assert.AreEqual(2, Stream.From<TPerson>(people) //
      .Filter(Person.Age >= 10).Count)

  finally
    people.Free;
  end;
end;

procedure TStreamTest.TestSort;
var
  people: TList<TPerson>;
  Person: TPersonMeta;
begin
  people := TList<TPerson>.Create;
  try
    people.Add(TPerson.Create(1, 'peter', 10, 0));
    people.Add(TPerson.Create(2, 'john', 15, 0));
    people.Add(TPerson.Create(3, 'mary', 8, 0));
    Person := Stream.ReflectMetadata<TPersonMeta, TPerson>();

    Assert.IsTrue(Stream.From<integer>([8, 10, 15]) //
      .Equals(Stream.From<TPerson>(people).map<integer>(
      function(const aperson: TPerson): integer
      begin
        result := aperson.Age;
      end).Sort()));

    Assert.IsTrue(Stream.From<integer>([15, 10, 8]) //
      .Equals(Stream.From<TPerson>(people).map<integer>(
      function(const aperson: TPerson): integer
      begin
        result := aperson.Age;
      end).Sort(DESC)));

    Assert.IsTrue(Stream.From<integer>([8, 10, 15]) //
      .Equals(Stream.From<TPerson>(people) //
      .SortBy(Person.Age) //
      .map<integer>(
      function(const aperson: TPerson): integer
      begin
        result := aperson.Age;
      end)));

    Assert.IsTrue(Stream.From<integer>([15, 10, 8]) //
      .Equals(Stream.From<TPerson>(people) //
      .SortBy(field(Person.Age, DESC)) //
      .map<integer>(
      function(const aperson: TPerson): integer
      begin
        result := aperson.Age;
      end)));

    Assert.IsTrue(Stream.From<string>(['john', 'mary', 'peter']) //
      .Equals(Stream.From<TPerson>(people).map<string>(
      function(const aperson: TPerson): string
      begin
        result := aperson.name;
      end).Sort()));

    Assert.IsTrue(Stream.From<string>(['peter', 'mary', 'john']) //
      .Equals(Stream.From<TPerson>(people).map<string>(
      function(const aperson: TPerson): string
      begin
        result := aperson.name;
      end).Sort(DESC)));

    Assert.IsTrue(Stream.From<string>(['john', 'mary', 'peter']) //
      .Equals(Stream.From<TPerson>(people) //
      .SortBy(Person.FirstName) //
      .map<string>(
      function(const aperson: TPerson): string
      begin
        result := aperson.name;
      end)));

    Assert.IsTrue(Stream.From<string>(['peter', 'mary', 'john']) //
      .Equals(Stream.From<TPerson>(people) //
      .SortBy(field(Person.FirstName, DESC)) //
      .map<string>(
      function(const aperson: TPerson): string
      begin
        result := aperson.name;
      end)));

  finally
    people.Free;
  end;
end;

procedure TStreamTest.TestTakeOneFound;
var
  people: TList<TPerson>;
  Person: TPersonMeta;
  john15: TPerson;
begin
  people := TList<TPerson>.Create;
  try
    people.Add(TPerson.Create(1, 'peter', 10, 0));
    people.Add(TPerson.Create(2, 'john', 15, 0));
    people.Add(TPerson.Create(3, 'mary', 8, 0));
    Person := Stream.ReflectMetadata<TPersonMeta, TPerson>();

    john15 := Stream.From<TPerson>(people) //
      .Filter((Person.FirstName = 'john') and (Person.Age = 15)) //
      .TakeOne();
    Assert.AreEqual('john', john15.name);
    Assert.AreEqual(15, john15.Age);
  finally
    people.Free;
  end;
end;

procedure TStreamTest.TestTakeOneNotFound;
var
  people: TList<TPerson>;
  Person: TPersonMeta;
  johnNotFound: TPerson;
begin
  people := TList<TPerson>.Create;
  try
    people.Add(TPerson.Create(1, 'peter', 10, 0));
    people.Add(TPerson.Create(2, 'john', 15, 0));
    people.Add(TPerson.Create(3, 'mary', 8, 0));
    Person := Stream.ReflectMetadata<TPersonMeta, TPerson>();

    johnNotFound := Stream.From<TPerson>(people) //
      .Filter((Person.FirstName = 'john') and (Person.Age = 10)) //
      .TakeOne();

  finally
    people.Free;
  end;

end;

procedure TStreamTest.TestToArrayAndList;
var
  people, lst: TList<TPerson>;
  Person: TPersonMeta;
begin
  people := TList<TPerson>.Create;
  try
    people.Add(TPerson.Create(1, 'peter', 10, 0));
    people.Add(TPerson.Create(2, 'john', 15, 0));
    people.Add(TPerson.Create(3, 'mary', 8, 0));
    Person := Stream.ReflectMetadata<TPersonMeta, TPerson>();

    Assert.AreEqual(3, integer(length(Stream.From<TPerson>(people) //
      .Filter(Person.Age >= 5).toArray)));
    lst := Stream.From<TPerson>(people).Filter(Person.Age >= 5).ToList;
    try
      Assert.AreEqual(3, lst.Count);
    finally
      lst.Free;
    end;
  finally
    people.Free;
  end;
end;

procedure TStreamTest.TestUnique;
begin
  Assert.IsTrue(Stream.From<integer>([1, 2, 3, 4, 5, 7]) //
    .Equals(Stream.From<integer>([5, 4, 2, 7, 3, 3, 2, 7, 1]).Unique));
end;

procedure TStreamTest.TestGroupBy;
var
  people: TList<TPerson>;
  Person: TPersonMeta;
  grouping: tdictionary<string, tarray<TPerson>>;
begin
  people := TList<TPerson>.Create;
  try
    people.Add(TPerson.Create(1, 'peter', 10, 0));
    people.Add(TPerson.Create(2, 'john', 15, 0));
    people.Add(TPerson.Create(3, 'mary', 8, 0));
    people.Add(TPerson.Create(4, 'peter', 20, 0));
    Person := Stream.ReflectMetadata<TPersonMeta, TPerson>();

    grouping := Stream.From<TPerson>(people) //
      .GroupToArray<string>(Person.FirstName);
    try
      Assert.AreEqual(2, integer(length(grouping.Items['peter'])));
      Assert.AreEqual(1, integer(length(grouping.Items['mary'])));
      Assert.AreEqual(1, integer(length(grouping.Items['john'])));
    finally
      grouping.Free;
    end;
  finally
    people.Free;
  end;
end;

{ TAddr }

constructor TAddr.Create(const aid: integer; const azip: string);
begin
  id := aid;
  zip := azip;
end;

{ TPerson }

constructor TPerson.Create(const aid: integer; const aname: string; const aage, aaddrid: integer);
begin
  id := aid;
  name := aname;
  Age := aage;
  addrid := aaddrid;
end;

initialization

TDUnitX.RegisterTestFixture(TStreamTest);

end.
