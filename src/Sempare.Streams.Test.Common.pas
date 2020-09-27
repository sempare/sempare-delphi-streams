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
unit Sempare.Streams.Test.Common;

interface

uses
  System.Generics.Collections,
  System.SysUtils,

  Sempare.Streams,
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

  TPersonContext = record
    name: TFieldExpression;
    value: TFieldExpression;
    addr: TFieldExpression;
    sugar: TFieldExpression;
    num: TFieldExpression;
  end;

  TJoinedPersons = record
    Person: TPerson;
    addr: TAddr;
  end;

  [TestFixture]
  TStreamsTestBase = class
  protected
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

  end;

implementation

uses
  System.classes,
  Sempare.Streams.Enum,
  Sempare.Streams.Types;

{ TStreamsTestBase }

function TStreamsTestBase.Createaddrs: TList<TAddr>;
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

function TStreamsTestBase.Createaddrs2: TList<TAddr>;
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

function TStreamsTestBase.CreatePeople: TList<TPerson>;
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

procedure TStreamsTestBase.Setup;
begin
  Fpeople := CreatePeople;
  Faddrs := Createaddrs;
  Faddrs2 := Createaddrs2;
end;

procedure TStreamsTestBase.Teardown;
begin
  Fpeople.Free;
  Faddrs.Free;
  Faddrs2.Free;
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

{ TAddr }

constructor TAddr.Create(const aid: integer; const azip: string);
begin
  id := aid;
  zip := azip;
end;

end.
