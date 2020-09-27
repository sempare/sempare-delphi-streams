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
unit Sempare.Streams;

interface

uses
  System.Generics.Collections,
  System.Generics.Defaults,
  System.TypInfo,
  System.Rtti,
  Sempare.Streams.Rtti,
  System.SysUtils,
  Sempare.Streams.Types,
  Sempare.Streams.Expr,
  Sempare.Streams.Filter,
  Sempare.Streams.Enum,
  Sempare.Streams.Sort;

const
  /// <summary>
  /// soAscending is a sorting option indicating the sorting direction.
  /// </summary>
  soAscending = Sempare.Streams.Types.TSortOrder.soAscending;
  /// <summary>
  /// soDescending is a sorting option indicating the sorting direction.
  /// </summary>
  soDescending = Sempare.Streams.Types.TSortOrder.soDescending;
  /// <summary>
  /// ASC is a sorting option (an alias for soAscending)
  /// </summary>
  ASC = Sempare.Streams.Types.TSortOrder.ASC;
  /// <summary>
  /// DESC is a sorting option (an alias for soDescending)
  /// </summary>
  DESC = Sempare.Streams.Types.TSortOrder.DESC;

type
  /// <summary>
  /// EStream is the general exception using in the streams library.
  /// </summary>
  EStream = Sempare.Streams.Types.EStream;
  /// <summary>
  /// EStreamReflect is an exception returned while reflecting metadata.
  /// </summary>
  EStreamReflect = Sempare.Streams.Types.EStreamReflect;
  /// <summary>
  /// EStreamItemNotFound is an exception identifying that a record was not found.
  /// </summary>
  EStreamItemNotFound = Sempare.Streams.Types.EStreamItemNotFound;

  /// <summary>
  /// TSortOrder is the enumeration of (soAscending, soDescending).
  /// </summary>
  TSortOrder = Sempare.Streams.Types.TSortOrder;

  /// <summary>
  /// TExpression is a record helper used for boolean expressions (and, or, and not).
  /// </summary>
  TExpression = record
  strict private
    FExpr: IExpr;
  public
    constructor Create(AExpr: IExpr);
    class operator LogicalAnd(const ALeft: boolean; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalAnd(const ALeft: TExpression; const ARight: boolean): TExpression; overload; static;
    class operator LogicalAnd(const ALeft: TExpression; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalOr(const ALeft: boolean; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalOr(const ALeft: TExpression; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalOr(const ALeft: TExpression; const ARight: boolean): TExpression; overload; static;
    class operator LogicalNot(const AExpr: TExpression): TExpression; overload; static;
    property Expr: IExpr read FExpr;
  end;

  /// <summary>
  /// TFieldExpression is a record helper used to represent binary expressions (=, &lt;&gt; &lt;=, &lt;, &gt;, &gt;=). <para/>
  /// <code>
  /// var fld : TFieldExpression := field('name') = 'sarah';
  /// </code>
  /// </summary>
  TFieldExpression = record
  strict private
    FExpr: IFieldExpr;
  public
    constructor Create(AExpr: IFieldExpr);

    class operator Implicit(const AField: string): TFieldExpression;
    class operator Implicit(const [ref] AField: TFieldExpression): string;

    class operator Equal(const AField, AValue: TFieldExpression): TExpression;
    class operator GreaterThan(const AField, AValue: TFieldExpression): TExpression;
    class operator GreaterThanOrEqual(const AField, AValue: TFieldExpression): TExpression;
    class operator LessThan(const AField, AValue: TFieldExpression): TExpression;
    class operator LessThanOrEqual(const AField, AValue: TFieldExpression): TExpression;
    class operator NotEqual(const AField, AValue: TFieldExpression): TExpression;

    class operator Equal(const AField: TFieldExpression; const AValue: double): TExpression;
    class operator GreaterThan(const AField: TFieldExpression; const AValue: double): TExpression;
    class operator GreaterThanOrEqual(const AField: TFieldExpression; const AValue: double): TExpression;
    class operator LessThan(const AField: TFieldExpression; const AValue: double): TExpression;
    class operator LessThanOrEqual(const AField: TFieldExpression; const AValue: double): TExpression;
    class operator NotEqual(const AField: TFieldExpression; const AValue: double): TExpression;

    class operator Equal(const AField: TFieldExpression; const AValue: boolean): TExpression;
    class operator GreaterThan(const AField: TFieldExpression; const AValue: boolean): TExpression;
    class operator GreaterThanOrEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
    class operator LessThan(const AField: TFieldExpression; const AValue: boolean): TExpression;
    class operator LessThanOrEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
    class operator NotEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;

    class operator Equal(const AField: TFieldExpression; const AValue: string): TExpression; overload; static;
    class operator GreaterThan(const AField: TFieldExpression; const AValue: string): TExpression; overload; static;
    class operator GreaterThanOrEqual(const AField: TFieldExpression; const AValue: string): TExpression; overload; static;
    class operator LessThan(const AField: TFieldExpression; const AValue: string): TExpression; overload; static;
    class operator LessThanOrEqual(const AField: TFieldExpression; const AValue: string): TExpression; overload; static;
    class operator NotEqual(const AField: TFieldExpression; const AValue: string): TExpression; overload; static;

    class operator NotEqual(const AField: TFieldExpression; const AValue: int64): TExpression; overload; static;
    class operator Equal(const AField: TFieldExpression; const AValue: int64): TExpression; overload; static;
    class operator LessThan(const AField: TFieldExpression; const AValue: int64): TExpression; overload; static;
    class operator LessThanOrEqual(const AField: TFieldExpression; const AValue: int64): TExpression; overload; static;
    class operator GreaterThan(const AField: TFieldExpression; const AValue: int64): TExpression; overload; static;
    class operator GreaterThanOrEqual(const AField: TFieldExpression; const AValue: int64): TExpression; overload; static;

    property FieldExpr: IFieldExpr read FExpr;
  end;

  /// <summary>
  /// TSortExpression is a record helper used to represent sort constaints. <para/>
  /// <code>
  /// var fld : TSortExpression := field('name', ASC) and field('age', DESC) ;
  /// </code>
  /// </summary>
  TSortExpression = record
  strict private
    FExprs: TArray<ISortExpr>;
  public
    constructor Create(AExpr: ISortExpr); overload;
    constructor Create(AExprs: TArray<ISortExpr>); overload;

    class operator Implicit(const AField: TFieldExpression): TSortExpression; static;
    class function Field(const AName: string; const AOrder: TSortOrder = soAscending): TSortExpression; static;
    class operator LogicalAnd(const ALeft: TSortExpression; const ARight: TSortExpression): TSortExpression; overload; static;

    property Exprs: TArray<ISortExpr> read FExprs;
  end;

  TStreamOperation<T> = record
  private
    FEnum: IEnum<T>;
    function IsCached: boolean;
  public
    class operator Implicit(Enum: IEnum<T>): TStreamOperation<T>; static;

    /// <summary>
    /// Unique using the default comparator
    /// <summary>
    function Unique(): TStreamOperation<T>; overload;

    /// <summary>
    /// Unique using a custom comparator
    /// <summary>
    function Unique(AComparator: IComparer<T>): TStreamOperation<T>; overload;

    /// <summary>
    /// SortBy sorts the stream using a sort expression. The elements must be a class or record.
    /// <summary>
    function SortBy(const AExpr: TSortExpression): TStreamOperation<T>;

    /// <summary>
    /// Sort sorts the stream the sort order and a comparator.
    /// Using soDescening reverses the result from the comparator.
    /// <summary>
    function Sort(const ADirection: TSortOrder = soAscending; AComparator: IComparer<T> = nil): TStreamOperation<T>; overload;

    /// <summary>
    /// Sort sorts the stream using a comparator.
    /// <summary>
    function Sort(AComparator: IComparer<T>): TStreamOperation<T>; overload;

    /// <summary>
    /// TakeOne returns a single element or raised EStreamItemNotFound
    /// <summary>
    function TakeOne: T;

    /// <summary>
    /// ToArray returns all the items from the stream into a dynarray (TArray).
    /// <summary>
    function ToArray(): TArray<T>;

    /// <summary>
    /// ToList returns all the items from the stream into a TList.
    /// <summary>
    function ToList(): TList<T>;

    /// <summary>
    /// Equals returns true if all the items match another stream.
    /// <summary>
    function Equals(const [ref] AOther: TStreamOperation<T>): boolean;

    /// <summary>
    /// Take indicates that at most ANumber of items will be returned.
    /// <summary>
    function Take(const ANumber: integer): TStreamOperation<T>; overload;

    /// <summary>
    /// Skip indicates that ANumber of items will be skipped initially.
    /// <summary>
    function Skip(const ANumber: integer): TStreamOperation<T>; overload;

    /// <summary>
    /// Map applies a function to the items in the stream, mapping items from one type to another.
    /// <summary>
    function Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TStreamOperation<TOutput>;

    /// <summary>
    /// Apply applies a procedure to the items in the stream.
    /// <summary>
    procedure Apply(const AFunction: TApplyFunction<T>);

    /// <summary>
    /// Creates a cache of the items at this point so that the items can be enumerated again
    /// without having to reapply any transformations.
    /// <summary>
    function Cache: TStreamOperation<T>;

    /// <summary>
    /// Perform an inner join on two streams, where items should match in both streams.
    /// <summary>
    function InnerJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;

    /// <summary>
    /// Perform an left join on two streams, where items if first stream are returned, optionally matching items in the second stream.
    /// <summary>
    function LeftJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;

    /// <summary>
    /// Perform an right join on two streams, where items if second stream are returned, optionally matching items in the first stream.
    /// <summary>
    function RightJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;

    /// <summary>
    /// Perform a full join on two streams - the union of left and right joins.
    /// <summary>
    function FullJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;

    /// <summary>
    /// Filters items in the stream based on filter critera. The items should be a record or a class.
    /// <summary>
    function Filter(const ACondition: TExpression): TStreamOperation<T>; overload;

    /// <summary>
    /// Filters items in the stream based on filter function.
    /// <summary>
    function Filter(const ACondition: TFilterFunction<T>): TStreamOperation<T>; overload;

    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function GroupBy<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, T>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function GroupBy<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function GroupToLists<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, TList<T>>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function GroupToLists<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function GroupToArray<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, TArray<T>>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function GroupToArray<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>; overload;

    /// <summary>
    /// Count the items in the stream.
    /// <summary>
    function Count: integer;

  end;

  /// <summary>
  /// Stream is the primary entry point for stream operations.
  /// <code>
  /// type <para/>
  /// TPerson = class<para/>
  /// private<para/>
  /// FFN : string;<para/>
  /// FLN : string;<para/>
  /// FAge : integer; <para/>
  /// public  <para/>
  /// property FirstName : string read FFN;<para/>
  /// end;<para/>
  /// </code>
  /// <code>
  /// <para/>
  /// [StreamRef(TPerson)]<para/>
  /// TPersonMeta = record<para/>
  /// [StreamRef('FFN')]<para/>
  /// FirstName : IFieldExpression;<para/>
  /// [StreamRef('FLN')]<para/>
  /// LastName : IFieldExpression;<para/>
  /// [StreamRef('FAge')]<para/>
  /// Age : IFieldExpression;<para/>
  /// end; <para/><para/>
  /// </code>
  /// <code>
  /// var Person : TPersonMeta = Stream.ReflectMeta&lt;TPersonMeta&gt;(); <para/>
  /// </code>
  /// </summary>
  Stream = record
  public

    /// <summary>
    /// Stream from a TEnumerable&lt;T&gt; source.
    /// </summary>
    /// <param name="ASource">A source of type TEnumerable&lt;T&gt;.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the TEnumerable source.</returns>
    class function From<T>(const ASource: TEnumerable<T>): TStreamOperation<T>; overload; static;

    /// <summary>
    /// Stream from a IEnumerable&lt;T&gt; source.
    /// </summary>
    /// <param name="ASource">A source of type IEnumerable&lt;T&gt;.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the IEnumerable source.</returns>
    class function From<T>(const ASource: IEnumerable<T>): TStreamOperation<T>; overload; static;

    /// <summary>
    /// Stream from a dynarray (TArray&lt;T&gt;) source.
    /// </summary>
    /// <param name="ASource">A source of type TArray&lt;T&gt;.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the TArray source.</returns>;
    class function From<T>(const ASource: TArray<T>): TStreamOperation<T>; overload; static;

    /// <summary>
    /// Reflect the metadata record from a given type.
    /// </summary>
    /// <returns>TMetadata containing fields of type TFieldExpression used to simplify queries.</returns>;
    class function ReflectMetadata<TMetadata: record; T>(): TMetadata; static;
  end;

  /// <summary>
  /// StreamRef attribute allows for referencing fields between a class definition and the metadata record.
  /// </summary>
  StreamFieldAttribute = class(TCustomAttribute)
  private
    FName: string;
  public
    constructor Create(const AName: string);
    property Name: string read FName;
  end;

function Field(const AName: string): TFieldExpression; overload;
function Field(const AName: string; const AOrder: TSortOrder): TSortExpression; overload;
function Self(): TFieldExpression;

implementation

function Self(): TFieldExpression;
begin
  result := '_';
end;

function Field(const AName: string): TFieldExpression;
begin
  result := AName;
end;

function Field(const AName: string; const AOrder: TSortOrder): TSortExpression;
begin
  result := TSortExpression.Field(AName, AOrder);
end;

{ TSortExpression }

constructor TSortExpression.Create(AExprs: TArray<ISortExpr>);
begin
  FExprs := AExprs;
end;

constructor TSortExpression.Create(AExpr: ISortExpr);
begin
  setlength(FExprs, 1);
  FExprs[0] := AExpr;
end;

class function TSortExpression.Field(const AName: string; const AOrder: TSortOrder): TSortExpression;
begin
  result := TSortExpression.Create(TSortExpr.Create(AName, AOrder));
end;

class operator TSortExpression.Implicit(const AField: TFieldExpression): TSortExpression;
begin
  result := TSortExpression.Create(TSortExpr.Create(AField.FieldExpr.Field, soAscending));
end;

class operator TSortExpression.LogicalAnd(const ALeft, ARight: TSortExpression): TSortExpression;
var
  e: ISortExpr;
begin
  result.FExprs := ALeft.FExprs;
  for e in ARight.FExprs do
    insert(e, result.FExprs, length(result.FExprs));
end;

{ TExpression }

constructor TExpression.Create(AExpr: IExpr);
begin
  FExpr := AExpr;
end;

class operator TExpression.LogicalAnd(const ALeft, ARight: TExpression): TExpression;
begin
  result.FExpr := TBinaryExpr.Create(ALeft.Expr, boAND, ARight.Expr);
end;

class operator TExpression.LogicalAnd(const ALeft: boolean; const ARight: TExpression): TExpression;
begin
  if not ALeft then
  begin
    result.FExpr := TBoolExpr.Create(false);
    exit;
  end;
  result.FExpr := ARight.Expr;
end;

class operator TExpression.LogicalAnd(const ALeft: TExpression; const ARight: boolean): TExpression;
begin
  if not ARight then
  begin
    result.FExpr := TBoolExpr.Create(false);
    exit;
  end;
  result.FExpr := ALeft.Expr;
end;

class operator TExpression.LogicalNot(const AExpr: TExpression): TExpression;
begin
  result.FExpr := TUnaryExpr.Create(AExpr.Expr, uoNOT);
end;

class operator TExpression.LogicalOr(const ALeft: boolean; const ARight: TExpression): TExpression;
begin
  if not ALeft then
  begin
    result.FExpr := TBoolExpr.Create(false);
    exit;
  end;
  result := ARight;
end;

class operator TExpression.LogicalOr(const ALeft: TExpression; const ARight: boolean): TExpression;
begin
  if not ARight then
  begin
    result.FExpr := TBoolExpr.Create(false);
    exit;
  end;
  result := ALeft;
end;

class operator TExpression.LogicalOr(const ALeft, ARight: TExpression): TExpression;
begin
  result.FExpr := TBinaryExpr.Create(ALeft.Expr, boOR, ARight.Expr);
end;

{ TFieldExpression }

class operator TFieldExpression.Implicit(const AField: string): TFieldExpression;
begin
  result.FExpr := TFieldExpr.Create(AField);
end;

function GetExpr(AExpr: IFieldExpr; const AOP: TFieldExprOper; const AValue: TValue): IFieldExpr; inline;
begin
  AExpr.OP := AOP;
  AExpr.Value := AValue;
  result := AExpr;
end;

constructor TFieldExpression.Create(AExpr: IFieldExpr);
begin
  FExpr := AExpr;
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGT, AValue));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue));
end;

class operator TFieldExpression.Implicit(const [ref] AField: TFieldExpression): string;
begin
  result := AField.FieldExpr.Field;
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLT, AValue));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foEQ, TValue.From<IFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGT, TValue.From<IFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGTE, TValue.From<IFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLT, TValue.From<IFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLTE, TValue.From<IFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foNEQ, TValue.From<IFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGT, AValue));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLT, AValue));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGT, AValue));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLT, AValue));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGT, AValue));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLT, AValue));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue));
end;

{ TStreamOperation<T> }

function TStreamOperation<T>.Cache: TStreamOperation<T>;
begin
  // we want to minimise caching if we can
  if IsCached then
    result := Self
  else
    result := Enum.Cache<T>(FEnum);
end;

function TStreamOperation<T>.Count: integer;
begin
  result := Enum.Count<T>(FEnum);
end;

function TStreamOperation<T>.Equals(const [ref] AOther: TStreamOperation<T>): boolean;
begin
  result := Enum.AreEqual<T>(FEnum, AOther.FEnum);
end;

class operator TStreamOperation<T>.Implicit(Enum: IEnum<T>): TStreamOperation<T>;
begin
  result.FEnum := Enum;
end;

function TStreamOperation<T>.GroupBy<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>;
begin
  result := Enum.GroupBy<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction);
end;

function TStreamOperation<T>.GroupBy<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, T>;
begin
  result := Enum.GroupBy<T, TKeyType>(FEnum, AField.FieldExpr);
end;

function TStreamOperation<T>.GroupToArray<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>;
begin
  result := Enum.GroupToArray<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction);
end;

function TStreamOperation<T>.GroupToArray<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, TArray<T>>;
begin
  result := Enum.GroupToArray<T, TKeyType>(FEnum, AField.FieldExpr);
end;

function TStreamOperation<T>.GroupToLists<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>;
begin
  result := Enum.GroupToLists<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction);
end;

function TStreamOperation<T>.GroupToLists<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, TList<T>>;
begin
  result := Enum.GroupToLists<T, TKeyType>(FEnum, AField.FieldExpr);
end;

function TStreamOperation<T>.IsCached: boolean;
begin
  result := supports(FEnum, IEnumCache<T>);
end;

function TStreamOperation<T>.InnerJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  result := TjoinEnum<T, TOther, TJoined>.Create( //
    FEnum, AOther.FEnum, AOn, ASelect);
end;

function TStreamOperation<T>.LeftJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  result := TLeftJoinEnum<T, TOther, TJoined>.Create(FEnum, AOther.FEnum, AOn, ASelect);
end;

function TStreamOperation<T>.Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TStreamOperation<TOutput>;
begin
  result := TMapEnum<T, TOutput>.Create(FEnum, AFunction);
end;

function TStreamOperation<T>.RightJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  result := AOther.LeftJoin<T, TJoined>(Self,
    function(const A: TOther; const B: T): boolean
    begin
      result := AOn(B, A);
    end,
    function(const A: TOther; const B: T): TJoined
    begin
      result := ASelect(B, A);
    end);
end;

function TStreamOperation<T>.Sort(const ADirection: TSortOrder; AComparator: IComparer<T>): TStreamOperation<T>;
var
  Comparer: IComparer<T>;
begin
  if AComparator = nil then
    Comparer := TComparer<T>.Default
  else
    Comparer := AComparator;
  if ADirection = soDescending then
    Comparer := TReverseComparer<T>.Create(Comparer);
  result := Sort(Comparer);
end;

function TStreamOperation<T>.Sort(AComparator: IComparer<T>): TStreamOperation<T>;
var
  rttiType: TRttiType;
  Comparer: IComparer<T>;
begin
  rttiType := rttictx.GetType(typeinfo(T));
  if (rttiType.TypeKind in [tkClass, tkRecord]) then
  begin
    raise EStream.Create('SortBy should be used on primitive types only');
  end
  else
  begin
    if AComparator = nil then
      Comparer := TComparer<T>.Default
    else
      Comparer := AComparator;
    result := TSortedEnum<T>.Create(FEnum, Comparer);
  end;
end;

function TStreamOperation<T>.SortBy(const AExpr: TSortExpression): TStreamOperation<T>;
var
  rttiType: TRttiType;
begin
  rttiType := rttictx.GetType(typeinfo(T));
  if (rttiType.TypeKind in [tkClass, tkRecord]) then
  begin
    result := TSortedEnum<T>.Create(FEnum, //
      TClassOrRecordComparer<T>.Create(AExpr.Exprs));
  end
  else
  begin
    raise EStream.Create('SortBy should be used on classes or records only');

    result := TSortedEnum<T>.Create(FEnum, //
      TComparer<T>.Default);
  end;
end;

function TStreamOperation<T>.Skip(const ANumber: integer): TStreamOperation<T>;
begin
  result := TSkip<T>.Create(FEnum, ANumber);
end;

function TStreamOperation<T>.Take(const ANumber: integer): TStreamOperation<T>;
begin
  result := ttake<T>.Create(FEnum, ANumber);
end;

function TStreamOperation<T>.TakeOne: T;
var
  res: TArray<T>;
begin
  res := Enum.ToArray<T>(ttake<T>.Create(FEnum, 1));
  if length(res) <> 1 then
    raise EStreamItemNotFound.Create('Expecting one item');
  result := res[0];
  res := nil;
end;

function TStreamOperation<T>.ToArray: TArray<T>;
begin
  result := Enum.ToArray<T>(FEnum);
end;

function TStreamOperation<T>.ToList: TList<T>;
begin
  result := Enum.ToList<T>(FEnum);
end;

function TStreamOperation<T>.Unique(AComparator: IComparer<T>): TStreamOperation<T>;
begin
  result := TUniqueEnum<T>.Create(FEnum, AComparator); //
end;

function TStreamOperation<T>.Unique: TStreamOperation<T>;
begin
  result := TUniqueEnum<T>.Create(FEnum, TComparer<T>.Default); //
end;

procedure TStreamOperation<T>.Apply(const AFunction: TApplyFunction<T>);
begin
  Enum.Apply<T>(FEnum, AFunction);
end;

function TStreamOperation<T>.Filter(const ACondition: TExpression): TStreamOperation<T>;
begin
  result := TFilterEnum<T>.Create(FEnum, //
    TExprFilter<T>.Create(ACondition.Expr) //
    );
end;

function TStreamOperation<T>.Filter(const ACondition: TFilterFunction<T>): TStreamOperation<T>;
begin
  result := TFilterEnum<T>.Create(FEnum, //
    TTypedFunctionFilter<T>.Create(ACondition) //
    );
end;

function TStreamOperation<T>.FullJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
var
  leftCache: TStreamOperation<T>;
  rightCache: TStreamOperation<TOther>;
begin
  leftCache := Enum.Cache<T>(FEnum);
  rightCache := Enum.Cache<TOther>(AOther.FEnum);
  result := TUnionEnum<TJoined>.Create( //
    [leftCache.LeftJoin<TOther, TJoined>(rightCache, AOn, ASelect).FEnum, //
    leftCache.RightJoin<TOther, TJoined>(rightCache, AOn, ASelect).FEnum //
    ]);
end;

{ Stream }

class function Stream.From<T>(const ASource: TArray<T>): TStreamOperation<T>;
begin
  result := TArrayEnum<T>.Create(ASource);
end;

class function Stream.From<T>(const ASource: TEnumerable<T>): TStreamOperation<T>;
begin
  result := TTEnumerableEnum<T>.Create(ASource);
end;

class function Stream.ReflectMetadata<TMetadata, T>: TMetadata;
var
  attrib: TCustomAttribute;
  metaType, otherType, fieldExprType: TRttiType;
  fld: TRttiField;
  Name: string;
  Expr: TFieldExpression;
  exprVal, resVal, v: TValue;
begin
  fillchar(result, sizeof(result), 0);
  fieldExprType := rttictx.GetType(typeinfo(TFieldExpression));
  metaType := rttictx.GetType(typeinfo(TMetadata));
  resVal := TValue.From<TMetadata>(result);

  // resolve otherType by searching for StreamClass on metaType
  otherType := rttictx.GetType(typeinfo(T));;

  // ensure all fields on metadata class are of type TFieldExpression
  // and validate the names
  for fld in metaType.GetFields do
  begin
    name := fld.Name;
    if fld.FieldType <> fieldExprType then
      raise EStreamReflect.CreateFmt('Metadata field ''%s'' should be ''%s''', [name, fieldExprType.Name]);

    for attrib in fld.GetAttributes do
    begin
      if attrib is StreamFieldAttribute then
      begin
        name := StreamFieldAttribute(attrib).Name;
        break;
      end;
    end;
    if otherType.GetField(name) = nil then
      raise EStreamReflect.CreateFmt('referenced field ''%s'' not found on ''%s''', [name, otherType.QualifiedName]);
    Expr := Field(name);
    exprVal := TValue.From<TFieldExpression>(Expr);
    fld.SetValue(@result, exprVal);
  end;
end;

class function Stream.From<T>(const ASource: IEnumerable<T>): TStreamOperation<T>;
begin
  result := TIEnumerableEnum<T>.Create(ASource);
end;

{ StreamFieldAttribute }

constructor StreamFieldAttribute.Create(const AName: string);
begin
  FName := AName;
end;

end.
