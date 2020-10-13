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
unit Sempare.Streams.SQL;

interface

uses
  System.Generics.Collections,
  System.Rtti,
  Sempare.Streams.Types,
  Sempare.Streams;

type
  Stream = Sempare.Streams.Stream;

  IStreamConnection = interface
    ['{7D4571B4-7FD3-49CA-8132-98741AB2AB1C}']
  end;

  ISQLBuilder = interface
    ['{6AC36EAA-CAAB-45E4-9CFD-F9A7D31206C6}']

    function From(const Atable: string; const AAlias: string = ''): ISQLBuilder;
    function InnerJoin(const Atable: string; const AAlias: string = ''): ISQLBuilder;
    function LeftJoin(const Atable: string; const AAlias: string = ''): ISQLBuilder;
    function RightJoin(const Atable: string; const AAlias: string = ''): ISQLBuilder;
    function FullJoin(const Atable: string; const AAlias: string = ''): ISQLBuilder;

    function Where(const AConditon : string) : ISQLBuilder;

    function OrderBy(const AExpr: TSortExpression) :ISQLBuilder;

    function Offset(const AOffset : int64) : ISQLBuilder;
    function Limit(const ALimit : int64) : ISQLBuilder;

    function GetParams: TArray<string>;
    function GetArgs: TArray<TValue>;
    function GetSQL: string;

  end;

  TStreamSQLSelectOperation = record
    function Select(const AFieldExpr: TArray<TFieldExpression>): TStreamSQLSelectOperation; overload;
    function Select(const AFieldExpr: TFieldExpression): TStreamSQLSelectOperation; overload;
    /// <summary>
    /// OrderBy sorts the stream using a sort expression. The elements must be a class or record.
    /// <summary>
    function OrderBy(const AExpr: TSortExpression): TStreamSQLSelectOperation;
    function Limit(const ALimit: integer): TStreamSQLSelectOperation;
    function Offset(const AOffset : integer): TStreamSQLSelectOperation;

    function ToArray<T>: TArray<T>;
    function ToList<T>: TList<T>;
    function TakeOne<T> : T;
  end;

  TStreamSQLJoinOperation = record
    function InnerJoin<T>: TStreamSQLJoinOperation; overload;
    function InnerJoin<T>(const AAlias: string): TStreamSQLJoinOperation; overload;
    function LeftJoin<T>: TStreamSQLJoinOperation; overload;
    function LeftJoin<T>(const AAlias: string): TStreamSQLJoinOperation; overload;
    function RightJoin<T>: TStreamSQLJoinOperation; overload;
    function RightJoin<T>(const AAlias: string): TStreamSQLJoinOperation; overload;
    function FullJoin<T>: TStreamSQLJoinOperation; overload;
    function FullJoin<T>(const AAlias: string): TStreamSQLJoinOperation; overload;

    /// <summary>
    /// Where items in the stream based on filter critera. The items should be a record or a class. (An alias for filter)
    /// <summary>
    function Where(const ACondition: TExpression): TStreamSQLSelectOperation; overload;

    function Count: int64;

  end;

  TStreamSQLFromOperation = record
    function From<T>: TStreamSQLJoinOperation; overload;
    function From<T>(const AAlias: string): TStreamSQLJoinOperation; overload;
  end;

  StreamSQLHelper = record helper for Stream

    /// <summary>
    /// Stream from a IEnumerable&lt;T&gt; source.
    /// </summary>
    /// <param name="ASource">A source of type IEnumerable&lt;T&gt;.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the IEnumerable source.</returns>
    class function Query(ASource: IStreamConnection): TStreamSQLFromOperation; overload; static;
  end;

implementation

{ StreamSQLHelper }

class function StreamSQLHelper.Query(ASource: IStreamConnection): TStreamSQLFromOperation;
begin

end;

{ TStreamSQLSelectOperation }

function TStreamSQLSelectOperation.Limit(const ALimit: integer): TStreamSQLSelectOperation;
begin

end;

function TStreamSQLSelectOperation.Offset(const AOffset: integer): TStreamSQLSelectOperation;
begin

end;

function TStreamSQLSelectOperation.OrderBy(const AExpr: TSortExpression): TStreamSQLSelectOperation;
begin

end;

function TStreamSQLSelectOperation.Select(const AFieldExpr: TFieldExpression): TStreamSQLSelectOperation;
begin

end;

function TStreamSQLSelectOperation.Select(const AFieldExpr: TArray<TFieldExpression>): TStreamSQLSelectOperation;
begin

end;

function TStreamSQLSelectOperation.TakeOne<T>: T;
begin

end;

function TStreamSQLSelectOperation.ToArray<T>: TArray<T>;
begin

end;

function TStreamSQLSelectOperation.ToList<T>: TList<T>;
begin

end;

{ TStreamSQLJoinOperation }

function TStreamSQLJoinOperation.Count: int64;
begin

end;

function TStreamSQLJoinOperation.FullJoin<T>(const AAlias: string): TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.FullJoin<T>: TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.InnerJoin<T>: TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.InnerJoin<T>(const AAlias: string): TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.LeftJoin<T>: TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.LeftJoin<T>(const AAlias: string): TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.RightJoin<T>: TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.RightJoin<T>(const AAlias: string): TStreamSQLJoinOperation;
begin

end;

function TStreamSQLJoinOperation.Where(const ACondition: TExpression): TStreamSQLSelectOperation;
begin

end;

{ TStreamSQLFromOperation }

function TStreamSQLFromOperation.From<T>(const AAlias: string): TStreamSQLJoinOperation;
begin

end;

function TStreamSQLFromOperation.From<T>: TStreamSQLJoinOperation;
begin

end;

end.
