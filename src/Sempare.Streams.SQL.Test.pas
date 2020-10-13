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
unit Sempare.Streams.SQL.Test;

interface

uses
  System.Generics.Collections,
  System.SysUtils,
  Sempare.Streams,
  Sempare.Streams.Test.Common,
  DUnitX.TestFramework;

type

  [TestFixture]
  TSQLStreamTest = class(TStreamsTestBase)
  public
    [Setup]
    procedure Setup; override;

    [Teardown]
    procedure Teardown; override;

    [Test]
    procedure TestSyntaxFlow;

  end;

type
  TPerson = record
    Id: integer;
    Name: string;
    AddrId: integer;
  end;

  TPersonMeta = record
    Id: TFieldExpression;
    Name: TFieldExpression;
    AddrId: TFieldExpression;
  end;

  TAddr = record
    Id: integer;
    Addr: string;
    Country: string;
  end;

  TAddrMeta = record
    Id: TFieldExpression;
    Addr: TFieldExpression;
    Country: TFieldExpression;
  end;

  TPersonAddr = record
    Id: integer;
    Name: string;
    AddrId: integer;
    Addr: string;
    Country: string;
  end;

  TPersonAddrMeta = record
    Id: TFieldExpression;
    Name: TFieldExpression;
    AddrId: TFieldExpression;
    Addr: TFieldExpression;
    Country: TFieldExpression;
  end;

implementation

uses
  Spring.Collections,
  Sempare.Streams.SQL;

{ TSQLStreamTest }

procedure TSQLStreamTest.Setup;
begin
  inherited;
end;

procedure TSQLStreamTest.Teardown;
begin
  inherited;

end;

procedure TSQLStreamTest.TestSyntaxFlow;
var
  connection: IStreamConnection;
  Person: TPersonMeta;
  Addr: TAddrMeta;
  count: integer;
  PersonResult: TPerson;
  PersonAddrResult: TPersonAddr;
  results: TArray<TPersonAddr>;
begin
  connection := nil;
  Person := Stream.ReflectMetadata<TPersonMeta, TPerson>();

  // syntax for counting
  count := Stream //
    .Query(connection) //
    .From<TPerson> //
    .count;

  // syntax for simple query
  PersonResult := Stream.Query(connection) //
    .From<TPerson> //
    .Where(Person.Name = 'john') //
    .TakeOne<TPerson>;

  // syntax for join
  PersonAddrResult := Stream.Query(connection) //
    .From<TPerson>('p') //
    .InnerJoin<TAddr>('a') //
    .Where(Person.Name = 'john') //
    .TakeOne<TPersonAddr>;

  // syntax for join
  results := Stream.Query(connection) //
    .From<TPerson>('p') //
    .InnerJoin<TAddr>('a') //
    .Where(Person.Name = 'john') //
    .Offset(1) //
    .Limit(10) //
    .OrderBy(Person.Name)
    .ToArray<TPersonAddr>;

end;

initialization

TDUnitX.RegisterTestFixture(TSQLStreamTest);

end.
