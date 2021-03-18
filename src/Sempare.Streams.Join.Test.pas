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
 * Copyright (c) 2020-2021 Sempare Limited                                    *
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
unit Sempare.Streams.Join.Test;

interface

uses
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams,
  Sempare.Streams.Test.Common,
  DUnitX.TestFramework;

type

  [TestFixture]
  TStreamsJoinTest = class(TStreamsTestBase)
  public
    [Setup]
    procedure Setup; override;

    [Teardown]
    procedure Teardown; override;

    [Test]
    procedure TestFullJoin;

    [Test]
    procedure TestJoin;

    [Test]
    procedure TestLeftJoin;

    [Test]
    procedure TestRightJoin;
  end;

implementation


procedure TStreamsJoinTest.Setup;
begin
  inherited;
end;

procedure TStreamsJoinTest.Teardown;
begin
  inherited;
end;

procedure TStreamsJoinTest.TestFullJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .FullJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs2), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      exit(a.addrid = b.id);
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(13, integer(length(res)));
end;

procedure TStreamsJoinTest.TestJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .InnerJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      exit(a.addrid = b.id);
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(4, integer(length(res)));
  Assert.AreEqual('john', res[0].Person.name);
  Assert.AreEqual(1, res[0].addr.id);
  Assert.AreEqual('mary', res[1].Person.name);
  Assert.AreEqual(2, res[1].addr.id);
  Assert.AreEqual('abraham', res[2].Person.name);
  Assert.AreEqual(4, res[2].addr.id);
  Assert.AreEqual('grant', res[3].Person.name);
  Assert.AreEqual(4, res[3].addr.id);
end;

procedure TStreamsJoinTest.TestLeftJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .LeftJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      exit(a.addrid = b.id);
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(7, integer(length(res)));

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

procedure TStreamsJoinTest.TestRightJoin;
var
  res: TArray<TJoinedPersons>;
begin
  res := Stream.From<TPerson>(Fpeople) //
    .RightJoin<TAddr, TJoinedPersons>(Stream.From<TAddr>(Faddrs2), //
    function(const a: TPerson; const b: TAddr): boolean
    begin
      exit(a.addrid = b.id);
    end,
    function(const a: TPerson; const b: TAddr): TJoinedPersons
    begin
      result.Person := a;
      result.addr := b;
    end).ToArray;
  Assert.AreEqual(6, integer(length(res)));

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

initialization

TDUnitX.RegisterTestFixture(TStreamsJoinTest);

end.
