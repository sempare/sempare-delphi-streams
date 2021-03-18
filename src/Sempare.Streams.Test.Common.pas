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

  TStreamsTestBase = class
  protected
    Fpeople: TArray<TPerson>;
    Faddrs: TArray<TAddr>;
    Faddrs2: TArray<TAddr>;
    FArr: TArray<TPerson>;
    fperson: TPerson;
    function CreatePeople: TArray<TPerson>;
    function Createaddrs: TArray<TAddr>;
    function Createaddrs2: TArray<TAddr>;

  public
    procedure Setup; virtual;

    procedure Teardown; virtual;

  end;

implementation

uses
  System.classes;

{ TStreamsTestBase }

function TStreamsTestBase.Createaddrs: TArray<TAddr>;
begin
  result := nil;
  insert(TAddr.Create(1, 'addr1'), result, length(result));
  insert(TAddr.Create(2, 'addr2'), result, length(result));
  insert(TAddr.Create(3, 'addr3'), result, length(result));
  insert(TAddr.Create(4, 'addr4'), result, length(result));
  insert(TAddr.Create(5, 'addr5'), result, length(result));
end;

function TStreamsTestBase.Createaddrs2: TArray<TAddr>;
begin
  result := nil;
  insert(TAddr.Create(0, 'addr0'), result, length(result));
  insert(TAddr.Create(2, 'addr2'), result, length(result));
  insert(TAddr.Create(7, 'addr7'), result, length(result));
  insert(TAddr.Create(8, 'addr8'), result, length(result));
end;

function TStreamsTestBase.CreatePeople: TArray<TPerson>;
begin
  result := nil;
  insert(TPerson.Create(1, 'peter', 10, '7700', false, 1.2, 0), result, length(result));
  insert(TPerson.Create(2, 'john', 15, '7705', false, 1.3, 1), result, length(result));
  insert(TPerson.Create(3, 'mary', 14, '7800', false, 1.1, 2), result, length(result));
  insert(TPerson.Create(4, 'matthew', 16, '8800', true, 1.2, 0), result, length(result));
  insert(TPerson.Create(5, 'abraham', 12, '8800', true, 1.3, 4), result, length(result));
  insert(TPerson.Create(6, 'grant', 16, '8845', true, 1.5, 4), result, length(result));
  insert(TPerson.Create(7, 'john', 17, '7805', false, 1.0, 0), result, length(result));
end;

procedure TStreamsTestBase.Setup;
begin
  Fpeople := CreatePeople;
  Faddrs := Createaddrs;
  Faddrs2 := Createaddrs2;
  FArr := nil;
  fillchar(fperson, sizeof(TPerson), 0);
end;

procedure TStreamsTestBase.Teardown;
begin
  Fpeople := nil;
  Faddrs := nil;
  Faddrs2 := nil;
  FArr := nil;
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
