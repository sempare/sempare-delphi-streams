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
unit Sempare.Streams.Filter.Test;

interface

uses
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams.Enum,
  Sempare.Streams.Test.Common,
  DUnitX.TestFramework;

type

  [TestFixture]
  TStreamsFilterTest = class(TStreamsTestBase)

  public
    [Setup]
    procedure Setup; override;

    [Teardown]
    procedure Teardown; override;

    [Test]
    procedure TestFilterinteger;
    [Test, Ignore]
    procedure TestFilterNestedRecord;
    [Test]
    procedure TestFilterNot;
    [Test]
    procedure TestFilterOr;
    [Test]
    procedure TestFilterSimpleSortBoolean;
    [Test]
    procedure TestFilterSimpleSortDouble;
    [Test]
    procedure TestFilterSimpleSortinteger;
    [Test]
    procedure TestFilterSimpleSortString;
    [Test]
    procedure TestFilterSkipAndTake;
    [Test]
    procedure TestFieldExpr;
    [Test]
    procedure TestFilterAnd;
    [Test]
    procedure TestFilterBoolean;
    [Test]
    procedure TestFilterDouble;
  end;

implementation

uses Sempare.Streams;

procedure TStreamsFilterTest.Setup;
begin
  inherited;

end;

procedure TStreamsFilterTest.Teardown;
begin
  inherited;

end;

procedure TStreamsFilterTest.TestFieldExpr;
var
  Person: TPersonContext;
begin
  Person := Stream.ReflectMetadata<TPersonContext, TPerson>;
  fperson := Stream.From<TPerson>(Fpeople) //
    .Filter((Person.name = 'john') and (Person.value = 15)).TakeOne;
  Assert.AreEqual('john', fperson.name);
end;

procedure TStreamsFilterTest.TestFilterAnd;
begin
  fperson := Stream.From<TPerson>(Fpeople).Filter((field('name') = 'john') and (field('value') = 15)).TakeOne;
  Assert.AreEqual('john', fperson.name);
end;

procedure TStreamsFilterTest.TestFilterBoolean;
begin
  Assert.AreEqual(3, Stream.From<TPerson>(Fpeople).Filter((field('sugar') = true)).Count);
  Assert.AreEqual(4, Stream.From<TPerson>(Fpeople).Filter((field('sugar') = false)).Count);
end;

procedure TStreamsFilterTest.TestFilterDouble;
begin
  Assert.AreEqual(3, Stream.From<TPerson>(Fpeople).Filter((field('num') >= 1.3) and (field('num') <= 1.5)).Count);
end;

procedure TStreamsFilterTest.TestFilterinteger;
begin
  Assert.AreEqual(2, Stream.From<TPerson>(Fpeople).Filter((field('value') = 16)).Count);
end;

procedure TStreamsFilterTest.TestFilterNestedRecord;
begin
  Assert.AreEqual(2, Stream.From<TPerson>(Fpeople).Filter((field('addr.zip') = '8800')).Count);

end;

procedure TStreamsFilterTest.TestFilterNot;
begin
  Assert.AreEqual(5, Stream.From<TPerson>(Fpeople).Filter(not(field('value') = 16)).Count);
end;

procedure TStreamsFilterTest.TestFilterOr;
begin
  Assert.AreEqual(4, Stream.From<TPerson>(Fpeople).Filter((field('name') = 'john') or (field('num') = 1.2)).Count);

end;

procedure TStreamsFilterTest.TestFilterSimpleSortBoolean;
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

procedure TStreamsFilterTest.TestFilterSimpleSortDouble;
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

procedure TStreamsFilterTest.TestFilterSimpleSortinteger;
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

procedure TStreamsFilterTest.TestFilterSimpleSortString;
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

procedure TStreamsFilterTest.TestFilterSkipAndTake;
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

initialization

TDUnitX.RegisterTestFixture(TStreamsFilterTest);

end.
