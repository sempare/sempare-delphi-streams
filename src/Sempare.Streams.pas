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
unit Sempare.Streams;

interface

{$I 'Sempare.Streams.inc'}

uses
  Data.DB,
  System.TypInfo,
  System.Rtti,
  System.SyncObjs,
{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}
  Spring.Collections,
{$ENDIF}
  System.SysUtils,
  System.Generics.Defaults,
  System.Generics.Collections;

type
  EStream = class(Exception);
  EStreamReflect = class(EStream);
  EStreamItemNotFound = class(EStream);

  TSortOrder = (soAscending, soDescending, ASC = soAscending, DESC = soDescending);

  TFilterFunction<TInput> = reference to function(const AInput: TInput): boolean;

  TJoinOnFunction<T, TOther> = reference to function(const A: T; const B: TOther): boolean;
  TJoinSelectFunction<T, TOther, TJoined> = reference to function(const A: T; const B: TOther): TJoined;
  TMapFunction<TInput, TOutput> = reference to function(const AInput: TInput): TOutput;

  // AInput is var rather than const [ref] to simplify what developers have to type.
  // It is a minor optimisation when records are used. Note that
  // you can change values in AInput, but changes to AInput itself will have no result.
  TApplyProc<TInput> = reference to procedure(var AInput: TInput);
  FValueFilter = reference to function(const AValue: TValue): boolean;

  TExprType = (etUnary, etBinary, etField, etBoolean, etFilter);

  IExpr = interface
    ['{90205885-54DA-4E53-A635-CC172BC19D15}']

    function IsTrue(const [ref] AValue: TValue): boolean;
    function GetExprType: TExprType;

    function IsExprType(const AExprType: TExprType): boolean;

    property ExprType: TExprType read GetExprType;

  end;

  IEnum<T> = interface
    ['{B5EAE436-8EE0-404A-B842-E6BD90B23E6F}']
    function EOF: boolean;
    procedure Next;
    function Current: T;
    function HasMore: boolean;
  end;

  IEnumCache<T> = interface
    ['{704AB8CE-4AD0-4166-A235-F0B2F8C0A20D}']
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  IFieldExtractor = interface
    ['{E62E9EBF-7686-40E2-8747-9255208923AD}']
    function GetValue(const AValue: TValue; var Value: TValue): boolean;
    function GetRttiFields: TArray<TRttiField>;
    function GetRttiType: TRttiType;
    property RttiFields: TArray<TRttiField> read GetRttiFields;
    property RttiType: TRttiType read GetRttiType;
  end;

  TFieldExprOper = (foEQ, foLT, foLTE, foGT, foGTE, foNEQ);

  IFieldExpr = interface(IExpr)
    ['{E99A1E39-384C-4323-8F5A-B08B3B2EEBAD}']

    function GetField: string;
    function GetOP: TFieldExprOper;
    function GetRttiField: IFieldExtractor;
    function GetValue: TValue;
    procedure SetOP(const Value: TFieldExprOper);
    procedure SetRttiField(const Value: IFieldExtractor);
    procedure SetValue(const Value: TValue);

    property Field: string read GetField;
    property OP: TFieldExprOper read GetOP write SetOP;
    property Value: TValue read GetValue write SetValue;
    property RttiField: IFieldExtractor read GetRttiField write SetRttiField;

  end;

  IFilterFunction = interface
    ['{E1073079-7967-4723-B6D4-6A9CB533DF30}']
    function IsTrue(const AValue: TValue): boolean;
  end;

  IFilterFunction<T> = interface(IFilterFunction)
    ['{0A3EE696-9714-4389-8EEB-CEF6B7748DD5}']
    function IsTrue(const AValue: T): boolean;
  end;

  /// <summary>
  /// StreamRef attribute allows for referencing fields between a class definition and the metadata record.
  /// </summary>
  StreamFieldAttribute = class(TCustomAttribute)
  private
    FName: string;
  public
    constructor Create(const AName: string);
    property name: string read FName;
  end;

type

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

  ISortExpr = interface
    ['{C16D3780-E9A6-412C-A589-958C8610AF3B}']
    function GetField: TRttiField;
    function GetName: string;
    function GetOrder: TSortOrder;
    procedure SetField(const Value: TRttiField);

    property name: string read GetName;
    property Order: TSortOrder read GetOrder;
    property RttiField: TRttiField read GetField write SetField;
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
    class operator Implicit(const [ref] OP: TStreamOperation<T>): IEnum<T>; static;

    /// <summary>
    /// Unique using the default comparator
    /// <summary>
    function Unique(): TStreamOperation<T>; overload;

    /// <summary>
    /// Unique using a custom comparator
    /// <summary>
    function Unique(AComparator: IComparer<T>): TStreamOperation<T>; overload;

    /// <summary>
    /// Distinct using the default comparator (Distinct is an alias for Unique)
    /// <summary>
    function Distinct(): TStreamOperation<T>; overload; inline;

    /// <summary>
    /// Distinct using a custom comparator  (Distinct is an alias for Unique)
    /// <summary>
    function Distinct(AComparator: IComparer<T>): TStreamOperation<T>; overload; inline;

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
    /// TryTakeOne returns a single element if present.
    /// <summary>
    function TryTakeOne(out AValue: T): boolean;

    /// <summary>
    /// ToArray returns all the items from the stream into a dynarray (TArray).
    /// <summary>
    function ToArray(): TArray<T>;

    /// <summary>
    /// ToList returns all the items from the stream into a TList.
    /// <summary>
    function ToList(): TList<T>;
{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}
    /// <summary>
    /// ToIList returns all the items from the stream into a Spring4d IList.
    /// <summary>
    function ToIList(): IList<T>;

    /// <summary>
    /// ToISet returns all the items from the stream into a Spring4d ISet.
    /// <summary>
    function ToISet(): ISet<T>;

{$ENDIF}
    /// <summary>
    /// Equals returns true if all the items match another stream.
    /// <summary>
    function Equals(const [ref] AOther: TStreamOperation<T>): boolean;

    /// <summary>
    /// Cast a stream from one type to another
    /// <summary>
    function CastTo<TOther: class>(): TStreamOperation<TOther>;

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
    procedure Apply(const AFunction: TApplyProc<T>);

    /// <summary>
    /// Update applies a procedure to the items in the stream. (Update is an alias for Apply);
    /// <summary>
    procedure Update(const AFunction: TApplyProc<T>); inline;

    /// <summary>
    /// Delete items from a target list based on values in the stream
    /// <summary>
    procedure Delete(const ATarget: TList<T>; AComparator: IComparer<T> = nil); overload;

    /// <summary>
    /// Delete items from a target array based on values in the stream
    /// <summary>
    procedure Delete(var ATarget: TArray<T>; AComparator: IComparer<T> = nil); overload;

    /// <summary>
    /// Delete items from a target dictionary based on key values in the stream
    /// <summary>
    procedure Delete<TValue>(const ATarget: TDictionary<T, TValue>); overload;

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
    /// Union with another stream so the results appear as one.
    /// <summary>
    function Union(const [ref] AOther: TStreamOperation<T>): TStreamOperation<T>;

    /// <summary>
    /// Filters items in the stream based on filter critera. The items should be a record or a class.
    /// <summary>
    function Filter(const ACondition: TExpression): TStreamOperation<T>; overload;

    /// <summary>
    /// Filters items in the stream based on filter function.
    /// <summary>
    function Filter(const ACondition: TFilterFunction<T>): TStreamOperation<T>; overload; inline;

    /// <summary>
    /// Where items in the stream based on filter critera. The items should be a record or a class. (An alias for filter)
    /// <summary>
    function Where(const ACondition: TExpression): TStreamOperation<T>; overload; inline;

    /// <summary>
    /// Where items in the stream based on filter function. (An alias for filter)
    /// <summary>
    function Where(const ACondition: TFilterFunction<T>): TStreamOperation<T>; overload;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function IGroupBy<TKeyType>(AField: TFieldExpression): IDictionary<TKeyType, T>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function IGroupBy<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, TValueType>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function IGroupToLists<TKeyType>(AField: TFieldExpression): IDictionary<TKeyType, IList<T>>; overload;
    /// <summary>
    /// Group by items matching a field expression. The TKeyType shoul match the type in the class or record.
    /// <summary>
    function IGroupToLists<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, IList<TValueType>>; overload;

{$ENDIF}
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
    /// Return  minimum value from a stream
    /// <summary>
    function Min(): T; overload;
    { /// <summary>
      /// Return  the minimum value from a stream
      /// <summary>
      function Min(const AComparer: TComparer<T>): T; overload; }
    /// <summary>
    /// Return  the minimum value from a stream
    /// <summary>
    function Min(AComparer: IComparer<T>): T; overload;

    /// <summary>
    /// Return  the maximum value from a stream
    /// <summary>
    function Max(): T; overload;
    { /// <summary>
      /// Return  the maximum value from a stream
      /// <summary>
      function Max(const AComparer: TComparer<T>): T; overload; }
    /// <summary>
    /// Return  the maximum value from a stream
    /// <summary>
    function Max(AComparer: IComparer<T>): T; overload;

    /// <summary>
    /// Return  true if the stream contains a value
    /// <summary>
    function Contains(const [ref] AValue: T): boolean; overload;
    /// <summary>
    /// Return  true if the stream contains a value
    /// <summary>
    function Contains(const [ref] AValue: T; AComparer: IComparer<T>): boolean; overload;
    { /// <summary>
      /// Return  true if the stream contains a value
      /// <summary>
      function Contains(const [ref] AValue: T; AComparer: IEqualityComparer<T>): boolean; overload;
      /// <summary>
      /// Return  true if the stream contains a value
      /// <summary>
      function Contains(const [ref] AValue: T; AComparer: TEqualityComparer<T>): boolean; overload;
    }
    /// <summary>
    /// Return  true when predicate true for all items in the stream
    /// <summary>
    function All(const APredicate: TPredicate<T>): boolean;
    /// <summary>
    /// Return  true when predicate true for any item in the stream
    /// <summary>
    function Any(const APredicate: TPredicate<T>): boolean;

    /// <summary>
    /// Return items in reverse order
    /// <summary>
    function Reverse(): TStreamOperation<T>;
    /// <summary>
    /// Return items in random order
    /// <summary>
    function Schuffle(): TStreamOperation<T>;

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
    /// Stream from a TDataSet source.
    /// </summary>
    /// <param name="ASource">A source of type TDataSet.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the TEnumerable source.</returns>
    class function From<T>(const ASource: TDataSet): TStreamOperation<T>; overload; static;

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
    class function From<T>(ASource: System.IEnumerable<T>): TStreamOperation<T>; overload; static;

    /// <summary>
    /// Stream from a dynarray (TArray&lt;T&gt;) source.
    /// </summary>
    /// <param name="ASource">A source of type TArray&lt;T&gt;.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the TArray source.</returns>;
    class function From<T>(const ASource: TArray<T>): TStreamOperation<T>; overload; static;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}
    /// <summary>
    /// Stream from a IEnumerable&lt;T&gt; source.
    /// </summary>
    /// <param name="ASource">A source of type IEnumerable&lt;T&gt;.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the IEnumerable source.</returns>
    class function From<T>(ASource: Spring.Collections.IEnumerable<T>): TStreamOperation<T>; overload; static;
{$ENDIF}
    /// <summary>
    /// Stream over chars in a string
    /// </summary>
    /// <param name="ASource">A source of type string.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the TArray source.</returns>;
    class function From(const ASource: string): TStreamOperation<char>; overload; static;

    /// <summary>
    /// Stream ints over a range
    /// </summary>
    /// <param name="AStart">start value</param>
    /// <param name="AEnd">end value</param>
    /// <param name="ADelta">delta applied to index until it exceeds AEnd</param>
    /// <returns>Range beteen AStart and AEnd in increments of ADelta.</returns>
    class function Range(const AStart, AEnd: int64; const ADelta: int64 = 1): TStreamOperation<int64>; overload; static;

    /// <summary>
    /// Stream extended over a range
    /// </summary>
    /// <param name="AStart">start value</param>
    /// <param name="AEnd">end value</param>
    /// <param name="ADelta">delta applied to index until it exceeds AEnd</param>
    /// <returns>Range beteen AStart and AEnd in increments of ADelta.</returns>
    class function Range(const AStart, AEnd: extended; const ADelta: extended = 1): TStreamOperation<extended>; overload; static;

    /// <summary>
    /// Reflect the metadata record from a given type.
    /// </summary>
    /// <returns>TMetadata containing fields of type TFieldExpression used to simplify queries.</returns>;
    class function ReflectMetadata<TMetadata: record; T>(): TMetadata; static;
  end;

  TStreamTypeCache = class
  strict private
    FLock: TCriticalSection;
    FMethods: TDictionary<ptypeinfo, TRttiInvokableType>;
    FTypes: TDictionary<ptypeinfo, TRttiType>;
    FExtractors: TDictionary<TArray<TRttiField>, IFieldExtractor>;
  public
    constructor Create;
    destructor Destroy; override;
    function GetMethod(const AInfo: ptypeinfo): TRttiInvokableType;
    function GetType(const AInfo: ptypeinfo): TRttiType;
    function GetExtractor(const A: TArray<TRttiField>): IFieldExtractor; overload;
    function GetExtractor(const AType: TRttiType; const A: string): IFieldExtractor; overload;
  end;

  TObjectHelper = class helper for TObject
  public
    class function SupportsInterface<TC: class; T: IInterface>(const AClass: TC): boolean; overload; static;
    class function SupportsInterface<T: IInterface>(out Intf: T): boolean; overload; static;
  end;

  TSortExpr = class(TInterfacedObject, ISortExpr)
  strict private
    FName: string;
    FOrder: TSortOrder;
    FField: TRttiField;
  private
    function GetField: TRttiField;
    function GetName: string;
    function GetOrder: TSortOrder;
    procedure SetField(const Value: TRttiField);
  public
    constructor Create(const AName: string; const AOrder: TSortOrder);
    property name: string read GetName;
    property Order: TSortOrder read GetOrder;
    property RttiField: TRttiField read GetField write SetField;
  end;

  TBaseComparer = class abstract(TInterfacedObject, IComparer<TValue>)
  public
    function Compare(const A, B: TValue): integer; virtual; abstract;
  end;

  TStringComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TIntegerComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TDoubleComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TBooleanComparer = class(TBaseComparer)
  public
    function Compare(const A, B: TValue): integer; override;
  end;

  TClassOrRecordComparer<T> = class abstract(TInterfacedObject, IComparer<T>)
  strict protected
    FComparators: TArray<IComparer<TValue>>;
    FExtractors: TArray<IFieldExtractor>;
    FExprs: TArray<ISortExpr>;
  public
    constructor Create(AExprs: TArray<ISortExpr>); overload;
    constructor Create(AComparators: TArray<IComparer<TValue>>; AExprs: TArray<ISortExpr>; AExtractors: TArray<IFieldExtractor>); overload;
    destructor Destroy; override;
    function Compare(const A, B: T): integer;
  end;

  TReverseComparer<T> = class(TComparer<T>)
  private
    FComparer: IComparer<T>;
  public
    constructor Create(Comparer: IComparer<T>);
    destructor Destroy; override;
    function Compare(const Left, Right: T): integer; override;
  end;

  TUnaryExpr = class;
  TBinaryExpr = class;
  TFieldExpr = class;
  TBoolExpr = class;

  TExprException = class(Exception);

  TExprVisitor = class
    procedure Visit(const AExpr: TBoolExpr); overload; virtual;
    procedure Visit(const AExpr: TUnaryExpr); overload; virtual;
    procedure Visit(const AExpr: TBinaryExpr); overload; virtual;
    procedure Visit(const AExpr: TFieldExpr); overload; virtual;
  end;

  IVisitableExpr = interface(IExpr)
    ['{C3242CA6-1996-41AB-BC8A-1281183DA76F}']
    procedure Accept(const AVisitor: TExprVisitor);
  end;

  TRttiExprVisitor = class(TExprVisitor)
  strict private
    FType: TRttiType;
  public
    constructor Create(const AType: TRttiType);
    procedure Visit(const AExpr: TFieldExpr); overload; override;
  end;

  TExpr = class abstract(TInterfacedObject, IExpr, IVisitableExpr)
  strict protected
    function GetExprType: TExprType; virtual; abstract;
  public
    procedure Accept(const AVisitor: TExprVisitor); virtual;
    function IsTrue(const [ref] AValue: TValue): boolean; virtual; abstract;

    function AsBoolExpr: TBoolExpr;
    function AsUnaryExpr: TUnaryExpr;
    function AsBinaryExpr: TBinaryExpr;
    function AsFieldExpr: TFieldExpr;
    function IsExprType(const AExprType: TExprType): boolean;

    property ExprType: TExprType read GetExprType;
  end;

  TBoolExpr = class(TExpr)
  strict private
    FValue: boolean;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(const AValue: boolean);
    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

  TUnaryExpr = class(TExpr)
  type
    TOper = (uoNOT);
  strict private
    FExpr: IExpr;
    FOP: TOper;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(AExpr: IExpr; const AOP: TOper);
    destructor Destroy; override;

    procedure Accept(const AVisitor: TExprVisitor); override;
    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

  TBinaryExpr = class(TExpr)
  type
    TOper = (boAND, boOR);
  strict private
    FLeft: IExpr;
    FOP: TOper;
    FRight: IExpr;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(ALeft: IExpr; const AOP: TOper; ARight: IExpr);
    destructor Destroy; override;
    procedure Accept(const AVisitor: TExprVisitor); override;
    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

  TFieldExpr = class(TExpr, IFieldExpr)
  strict private
    FField: string;
    FOP: TFieldExprOper;
    FValue: TValue;
    FRttiField: IFieldExtractor;
    function GetField: string;
    function GetOP: TFieldExprOper;
    function GetRttiField: IFieldExtractor;
    function GetValue: TValue;
    procedure SetOP(const Value: TFieldExprOper);
    procedure SetRttiField(const Value: IFieldExtractor);
    procedure SetValue(const Value: TValue);
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(const AField: string);
    destructor Destroy; override;
    function IsTrue(const [ref] AValue: TValue): boolean; override;
    property Field: string read GetField;
    property OP: TFieldExprOper read GetOP write SetOP;
    property Value: TValue read GetValue write SetValue;
    property RttiField: IFieldExtractor read GetRttiField write SetRttiField;
  end;

  TFilterExpr = class(TExpr)
  strict private
    FExpr: IFilterFunction;
  strict protected
    function GetExprType: TExprType; override;
  public
    constructor Create(Expr: IFilterFunction);
    destructor Destroy; override;

    function IsTrue(const [ref] AValue: TValue): boolean; override;
  end;

  TAbstractFilter<T> = class abstract(TInterfacedObject, IFilterFunction, IFilterFunction<T>)
  public
    function IsTrue(const AData: TValue): boolean; overload; virtual; abstract;
    function IsTrue(const AData: T): boolean; overload;
  end;

  TExprFilter<T> = class(TAbstractFilter<T>)
  strict private
    FExpr: IExpr;
  public
    constructor Create(AExpr: IExpr);
    destructor Destroy(); override;
    function IsTrue(const AData: TValue): boolean; overload; override;
  end;

  TTypedFunctionFilter<T> = class(TAbstractFilter<T>)
  strict private
    FFunction: TFilterFunction<T>;
  public
    constructor Create(const AFunction: TFilterFunction<T>);
    function IsTrue(const AData: TValue): boolean; override;
  end;

  TComparer<T> = reference to function(const Left, Right: T): integer;
  TEqualityComparer<T> = reference to function(const [ref] AValue, BValue: T): boolean;

  /// <summary>
  /// Enum is a utility class for enumerable operations
  /// <summary>
  Enum = class
  public
{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}
    class procedure CopyFromEnum<T>(ASource: IEnum<T>; ATarget: Spring.Collections.ICollection<T>); static;
    class function FromSpring4D<T>(ASource: Spring.Collections.IEnumerable<T>): IEnum<T>; static;
{$ENDIF}
    class function AreEqual<T>(AEnumA, AEnumB: IEnum<T>): boolean; overload; static;
    class function AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: IComparer<T>): boolean; overload; static;
    class function AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: IEqualityComparer<T>): boolean; overload; static;
    class function AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: TEqualityComparer<T>): boolean; overload; static;
    class function Count<T>(AEnum: IEnum<T>): integer; static;
    class function ToArray<T>(AEnum: IEnum<T>): TArray<T>; static;
    class function ToList<T>(AEnum: IEnum<T>): TList<T>; static;
    class procedure Apply<T>(AEnum: IEnum<T>; const AFunc: TApplyProc<T>); static;
    class function IsCached<T>(AEnum: IEnum<T>): boolean; static;
    class function TryGetCached<T>(AEnum: IEnum<T>; out ACachedEnum: IEnum<T>): boolean; static;
    class function Cache<T>(AEnum: IEnum<T>): IEnum<T>; overload; static;
    class function Cache<T>(AEnum: TList<T>): IEnum<T>; overload; static;
    class function Cache<T>(AEnum: TEnumerable<T>): IEnum<T>; overload; static;
    class function Map<T, TOutput>(AEnum: IEnum<T>; const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>; static;

    // numeric
    class function Min<T>(AEnum: IEnum<T>): T; overload; static;
    class function Max<T>(AEnum: IEnum<T>): T; overload; static;

    class function Min<T>(AEnum: IEnum<T>; const AComparer: TComparer<T>): T; overload; static;
    class function Max<T>(AEnum: IEnum<T>; const AComparer: TComparer<T>): T; overload; static;
    class function Min<T>(AEnum: IEnum<T>; AComparer: IComparer<T>): T; overload; static;
    class function Max<T>(AEnum: IEnum<T>; AComparer: IComparer<T>): T; overload; static;
    class function Sum(AEnum: IEnum<extended>): extended; overload; static;
    class function Sum(AEnum: IEnum<int64>): int64; overload; static;
    class function Average(AEnum: IEnum<extended>): extended; overload; static;
    class function Average(AEnum: IEnum<int64>): extended; overload; static;

    // boolean
    class function Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T): boolean; overload; static;
    class function Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T; AComparer: IComparer<T>): boolean; overload; static;
    class function Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T; AComparer: IEqualityComparer<T>): boolean; overload; static;
    class function Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T; AComparer: TEqualityComparer<T>): boolean; overload; static;

    class function All<T>(AEnum: IEnum<T>; const APredicate: TPredicate<T>): boolean; static;
    class function Any<T>(AEnum: IEnum<T>; const APredicate: TPredicate<T>): boolean; static;

    // misc

    class function Reverse<T>(AEnum: IEnum<T>): IEnum<T>;
    class function Schuffle<T>(AEnum: IEnum<T>): IEnum<T>;

    class function Cast<TInput; TOutput: class>(AEnum: IEnum<TInput>): IEnum<TOutput>;

    class procedure Delete<T>(AEnum: IEnum<T>; const ATarget: TList<T>; AComparator: IComparer<T>); overload;
    class procedure Delete<T>(AEnum: IEnum<T>; var ATarget: TArray<T>; AComparator: IComparer<T>); overload;
    class procedure Delete<T, TValue>(AEnum: IEnum<T>; const ATarget: TDictionary<T, TValue>); overload;

    // grouping
{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}
    class function IGroupBy<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): IDictionary<TKeyType, T>; overload; static;
    class function IGroupBy<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, TValueType>; overload; static;
    class function IGroupToLists<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): IDictionary<TKeyType, IList<T>>; overload; static;
    class function IGroupToLists<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, IList<TValueType>>;
      overload; static;
{$ENDIF}
    class function GroupBy<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, T>; overload; static;
    class function GroupBy<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>; overload; static;
    class function GroupToLists<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TList<T>>; overload; static;
    class function GroupToLists<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>; overload; static;
    class function GroupToArray<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TArray<T>>; overload; static;
    class function GroupToArray<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>;
      overload; static;
  end;

  /// <summary>
  /// THasMore is a base class for all enumerable classes. There are two ways the instances
  /// can be used.
  /// <code>
  /// while enum.HasMore do
  /// doSomething(enum.Current);
  /// </code>
  /// or
  /// <code>
  /// while not enum.EOF do
  /// begin
  /// doSomething(enum.Current);
  /// enum.Next;
  /// end;
  /// </code>
  /// <summary>
  THasMore<T> = class abstract(TInterfacedObject, IEnum<T>)
  private
    FFirst: boolean;
  public
    constructor Create;
    function EOF: boolean; virtual; abstract;
    procedure Next; virtual; abstract;
    function Current: T; virtual; abstract;
    function HasMore: boolean;
  end;

  /// <summary>
  /// TArrayEnum is an enumerator over an dynarray (TArray)
  /// </summary>
  TArrayEnum<T> = class(THasMore<T>, IEnumCache<T>)
  private
    FData: TArray<T>;
    FOffset: integer;
  public
    constructor Create(const AData: TArray<T>);
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  TSortedStatus = (ssSorted, ssUnsorted, ssUnknown);

  ISortedEnum = interface
    ['{4D59F9E5-7F74-455E-97EF-4FA6842B4FD0}']
    function GetSortedStatus: TSortedStatus;
  end;

  /// <summary>
  /// TTEnumerableEnum is an enumerator over a TEnumerable
  /// </summary>
  TTEnumerableEnum<T> = class(THasMore<T>, ISortedEnum)
  private
    FEnum: TEnumerator<T>;
    FEof: boolean;
    FSortedStatus: TSortedStatus;
  public
    constructor Create(const AEnum: TEnumerable<T>; ASortedStatus: TSortedStatus = ssUnknown);
    destructor Destroy; override;
    function GetSortedStatus: TSortedStatus;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TIEnumerableEnum is an enumerator over an IEnumerable
  /// </summary>
  TIEnumerableEnum<T> = class(THasMore<T>)
  private
    FEnum: System.IEnumerator<T>;
    FEof: boolean;
  public
    constructor Create(const AEnum: System.IEnumerable<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}

  /// <summary>
  /// TSpringIEnumerableEnum is an enumerator over a Spring4d IEnumerable
  /// </summary>
  TSpringIEnumerableEnum<T> = class(THasMore<T>)
  private
    FEnum: Spring.Collections.IEnumerator<T>;
    FEof: boolean;
  public
    constructor Create(const AEnum: Spring.Collections.IEnumerable<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;
{$ENDIF}

  /// <summary>
  /// TBaseEnum is a wrapper around another enumerator
  /// </summary>
  TBaseEnum<T> = class abstract(THasMore<T>)
  protected
    FEnum: IEnum<T>;
  public
    constructor Create(AEnum: IEnum<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    procedure Next; override;
    function Current: T; override;
  end;

  /// <summary>
  /// TSortedEnum ensures items are sorted.
  /// </summary>
  TSortedEnum<T> = class(TBaseEnum<T>, IEnumCache<T>)
  private
    FItems: TList<T>;
  public
    constructor Create(Enum: IEnum<T>; comparator: IComparer<T>);
    destructor Destroy; override;
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  /// <summary>
  /// TUniqueEnum ensures items are unique.
  /// </summary>
  TUniqueEnum<T> = class(TBaseEnum<T>, IEnumCache<T>)
  private
    FItems: TList<T>;
  public
    constructor Create(Enum: IEnum<T>; comparator: IComparer<T>);
    destructor Destroy; override;
    function GetEnum: IEnum<T>;
    function GetCache: TList<T>;
  end;

  /// <summary>
  /// TUnionEnum allows multiple enums be seen as a single stream.
  /// </summary>
  TUnionEnum<T> = class(THasMore<T>)
  private
    FEnums: TArray<IEnum<T>>;
    FIdx: integer;
  public
    constructor Create(Enum: TArray < IEnum < T >> );
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TIntRange allows us to enumerate a range of ints
  /// </summary>
  TIntRangeEnum = class(THasMore<int64>)
  private
    FIdx: int64;
    FEnd, FDelta: int64;
  public
    constructor Create(const AStart, AEnd: int64; const ADelta: int64 = 1);
    function EOF: boolean; override;
    function Current: int64; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TFloatRange allows us to enumerate a range of ints
  /// </summary>
  TFloatRangeEnum = class(THasMore<extended>)
  private
    FIdx: extended;
    FEnd, FDelta: extended;
  public
    constructor Create(const AStart, AEnd: extended; const ADelta: extended = 1);
    function EOF: boolean; override;
    function Current: extended; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TStringEnum allows us to enumerate a range of ints
  /// </summary>
  TStringEnum = class(THasMore<char>)
  private
    FValue: string;
    FIdx, FEnd: int64;
  public
    constructor Create(const AValue: string);
    function EOF: boolean; override;
    function Current: char; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TFilterEnum only returns items that for which the filter functions returns true.
  /// </summary>
  TFilterEnum<T> = class(TBaseEnum<T>)
  private
    FFilter: IFilterFunction<T>;
    FNext: T;
    FHasValue: boolean;
  public
    constructor Create(AEnum: IEnum<T>; const AFilter: TFilterFunction<T>); overload;
    constructor Create(AEnum: IEnum<T>; const AFilter: IFilterFunction<T>); overload;
    destructor Destroy; override;
    function EOF: boolean; override;
    function Current: T; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TSkip skips the first few items.
  /// </summary>
  TSkip<T> = class(TBaseEnum<T>)
  public
    constructor Create(AEnum: IEnum<T>; const ASkip: integer);
  end;

  /// <summary>
  /// TTake identifies how many items should be returned.
  /// </summary>
  TTake<T> = class(TBaseEnum<T>)
  private
    FTake: integer;
    FEof: boolean;
  public
    constructor Create(AEnum: IEnum<T>; const ATake: integer);
    function EOF: boolean; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TApplyEnum applies a procedure to each item in a stream
  /// </summary>
  TApplyEnum<T> = class(TBaseEnum<T>)
  private
    FApply: TApplyProc<T>;
  public
    constructor Create(AEnum: IEnum<T>; const AApply: TApplyProc<T>);
    function Current: T; override;
  end;

  /// <summary>
  /// TEnumCache contains an array of items. GetEnum returns a TCachedEnum
  /// </summary>
  TEnumCache<T> = class(TInterfacedObject, IEnumCache<T>)
  private
    FCache: TList<T>;
    FOwn: boolean;
  public
    constructor Create(AEnum: TList<T>); overload;
    constructor Create(AEnum: TEnumerable<T>); overload;
    constructor Create(AEnum: IEnum<T>; const AOwn: boolean = false); overload;
    destructor Destroy; override;
    function GetCache: TList<T>;
    function GetEnum: IEnum<T>;
  end;

  /// <summary>
  /// TCachedEnum enumerates items in the cache
  /// </summary>
  TCachedEnum<T> = class(THasMore<T>, IEnumCache<T>)
  private
    FCache: IEnumCache<T>;
    FEnum: IEnum<T>;
  public
    constructor Create(Cache: IEnumCache<T>);
    destructor Destroy; override;
    function EOF: boolean; override;
    procedure Next; override;
    function Current: T; override;
    function GetCache: TList<T>;
    function GetEnum: IEnum<T>;
  end;

  /// <summary>
  /// TMapEnum applies a function to items in the stream.
  /// </summary>
  TMapEnum<TInput, TOutput> = class(THasMore<TOutput>)
  private
    FEnum: IEnum<TInput>;
    FMapper: TMapFunction<TInput, TOutput>;
  public
    constructor Create(AEnum: IEnum<TInput>; AMapper: TMapFunction<TInput, TOutput>);
    destructor Destroy; override;
    function Current: TOutput; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TJoinEnum allows for an 'inner join' to be done between two streams.
  /// A matching function is required as well as a function that joins matching results.
  /// </summary>
  TJoinEnum<TLeft, TRight, TJoined> = class(THasMore<TJoined>)
  private
    FEnumLeft: IEnum<TLeft>;
    FEnumRight: IEnum<TRight>;
    FOn: TJoinOnFunction<TLeft, TRight>;
    FSelect: TJoinSelectFunction<TLeft, TRight, TJoined>;
    FHasLeft, FHasRight: boolean;
    FNext: TJoined;
    FHasNext: boolean;
    procedure FindNext;
    procedure ResetRight;
  public
    constructor Create( //
      AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; //
      const AOn: TJoinOnFunction<TLeft, TRight>; const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
    destructor Destroy; override;
    function Current: TJoined; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TLeftJoinEnum allows for an 'left join' to be done between two streams.
  /// A matching function is required as well as a function that joins matching results.
  /// </summary>
  TLeftJoinEnum<TLeft, TRight, TJoined> = class(THasMore<TJoined>)
  private
    FEnumLeft: IEnum<TLeft>;
    FEnumRight: IEnum<TRight>;
    FOn: TJoinOnFunction<TLeft, TRight>;
    FSelect: TJoinSelectFunction<TLeft, TRight, TJoined>;
    FHasLeft, FHasRight: boolean;
    FNext: TJoined;
    FHasNext: boolean;
    FFoundRight: boolean;
    procedure FindNext;
    procedure ResetRight;
  public
    constructor Create( //
      AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; //
      const AOn: TJoinOnFunction<TLeft, TRight>; const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
    destructor Destroy; override;
    function Current: TJoined; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TDataSetEnum enumerates a dataset with results into a managed type of T.
  /// </summary>
  TDataSetEnumRecord<T> = class(THasMore<T>)
  private
    FDataSet: TDataSet;
    FFields: TDictionary<string, string>;
    FRttiType: TRttiType;
  public
    constructor Create(const ADataSet: TDataSet);
    destructor Destroy; override;
    function Current: T; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  /// <summary>
  /// TDataSetEnum enumerates a dataset with results into a class of type T.
  /// </summary>
  TDataSetEnumClass<T> = class(THasMore<T>)
  private
    FDataSet: TDataSet;
    FConstructor: TRttiMethod;
    FFields: TDictionary<string, string>;
    FRttiType: TRttiType;
  public
    constructor Create(const ADataSet: TDataSet);
    destructor Destroy; override;
    function Current: T; override;
    function EOF: boolean; override;
    procedure Next; override;
  end;

  TArrayHelper = class helper for TArray
    class function IndexOf<T>(const ATarget: TArray<T>; const [ref] AValue: T; AComparator: IComparer<T>; out idx: integer): boolean; static;
  end;

function GetFieldsFromString(const AType: TRttiType; const A: string): TArray<TRttiField>;

function GetInterfaceTypeInfo(InterfaceTable: PInterfaceTable): ptypeinfo;

function Field(const AName: string): TFieldExpression; overload;
function Field(const AName: string; const AOrder: TSortOrder): TSortExpression; overload;
function Self(): TFieldExpression;

var
  RttiCtx: TRttiContext;
  StreamCache: TStreamTypeCache;
  SortString: IComparer<TValue>;
  SortInt64: IComparer<TValue>;
  SortDouble: IComparer<TValue>;
  SortBoolean: IComparer<TValue>;

implementation

function Self(): TFieldExpression;
begin
  exit('_');
end;

function Field(const AName: string): TFieldExpression;
begin
  exit(AName);
end;

function Field(const AName: string; const AOrder: TSortOrder): TSortExpression;
begin
  exit(TSortExpression.Field(AName, AOrder));
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
  exit(TSortExpression.Create(TSortExpr.Create(AName, AOrder)));
end;

class operator TSortExpression.Implicit(const AField: TFieldExpression): TSortExpression;
begin
  exit(TSortExpression.Create(TSortExpr.Create(AField.FieldExpr.Field, soAscending)));
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
  exit(ARight);
end;

class operator TExpression.LogicalOr(const ALeft: TExpression; const ARight: boolean): TExpression;
begin
  if not ARight then
  begin
    result.FExpr := TBoolExpr.Create(false);
    exit;
  end;
  exit(ALeft);
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
  exit(AExpr);
end;

constructor TFieldExpression.Create(AExpr: IFieldExpr);
begin
  FExpr := AExpr;
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue)));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGT, AValue)));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue)));
end;

class operator TFieldExpression.Implicit(const [ref] AField: TFieldExpression): string;
begin
  exit(AField.FieldExpr.Field);
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLT, AValue)));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue)));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: int64): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue)));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foEQ, TValue.From<IFieldExpr>(AValue.FExpr))));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGT, TValue.From<IFieldExpr>(AValue.FExpr))));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGTE, TValue.From<IFieldExpr>(AValue.FExpr))));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLT, TValue.From<IFieldExpr>(AValue.FExpr))));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLTE, TValue.From<IFieldExpr>(AValue.FExpr))));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foNEQ, TValue.From<IFieldExpr>(AValue.FExpr))));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue)));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGT, AValue)));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue)));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLT, AValue)));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue)));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: string): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue)));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue)));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGT, AValue)));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue)));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLT, AValue)));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue)));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: double): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue)));
end;

class operator TFieldExpression.Equal(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foEQ, AValue)));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGT, AValue)));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foGTE, AValue)));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLT, AValue)));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foLTE, AValue)));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: boolean): TExpression;
begin
  exit(TExpression.Create(GetExpr(AField.FExpr, foNEQ, AValue)));
end;

{ TStreamOperation<T> }

function TStreamOperation<T>.Cache: TStreamOperation<T>;
begin
  // we want to minimise caching if we can
  if IsCached then
    exit(Self)
  else
    exit(Enum.Cache<T>(FEnum));
end;

function TStreamOperation<T>.CastTo<TOther>: TStreamOperation<TOther>;
begin
  exit(Enum.Cast<T, TOther>(FEnum));
end;

function TStreamOperation<T>.Contains(const [ref] AValue: T; AComparer: IComparer<T>): boolean;
begin
  exit(Enum.Contains<T>(FEnum, AValue, AComparer));
end;
{
  function TStreamOperation<T>.Contains(const [ref] AValue: T; AComparer: IEqualityComparer<T>): boolean;
  begin
  exit(Enum.Contains<T>(FEnum, AValue, AComparer));
  end;

  function TStreamOperation<T>.Contains(const [ref] AValue: T; AComparer: TEqualityComparer<T>): boolean;
  begin
  exit(Enum.Contains<T>(FEnum, AValue, AComparer));
  end; }

function TStreamOperation<T>.Contains(const [ref] AValue: T): boolean;
begin
  exit(Enum.Contains<T>(FEnum, AValue));
end;

function TStreamOperation<T>.Count: integer;
begin
  exit(Enum.Count<T>(FEnum));
end;

procedure TStreamOperation<T>.Delete(const ATarget: TList<T>; AComparator: IComparer<T>);
begin
  Enum.Delete<T>(FEnum, ATarget, AComparator);
end;

procedure TStreamOperation<T>.Delete(var ATarget: TArray<T>; AComparator: IComparer<T>);
begin
  Enum.Delete<T>(FEnum, ATarget, AComparator);
end;

procedure TStreamOperation<T>.Delete<TValue>(const ATarget: TDictionary<T, TValue>);
begin
  Enum.Delete<T, TValue>(FEnum, ATarget);
end;

function TStreamOperation<T>.Distinct(AComparator: IComparer<T>): TStreamOperation<T>;
begin
  exit(Unique(AComparator));
end;

function TStreamOperation<T>.Distinct: TStreamOperation<T>;
begin
  exit(Distinct);
end;

function TStreamOperation<T>.Equals(const [ref] AOther: TStreamOperation<T>): boolean;
begin
  exit(Enum.AreEqual<T>(FEnum, AOther.FEnum));
end;

class operator TStreamOperation<T>.Implicit(Enum: IEnum<T>): TStreamOperation<T>;
begin
  result.FEnum := Enum;
end;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}

function TStreamOperation<T>.IGroupBy<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, TValueType>;
begin
  exit(Enum.IGroupBy<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction));
end;

function TStreamOperation<T>.IGroupBy<TKeyType>(AField: TFieldExpression): IDictionary<TKeyType, T>;
begin
  exit(Enum.IGroupBy<T, TKeyType>(FEnum, AField.FieldExpr));
end;

function TStreamOperation<T>.IGroupToLists<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, IList<TValueType>>;
begin
  exit(Enum.IGroupToLists<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction));
end;

function TStreamOperation<T>.IGroupToLists<TKeyType>(AField: TFieldExpression): IDictionary<TKeyType, IList<T>>;
begin
  exit(Enum.IGroupToLists<T, TKeyType>(FEnum, AField.FieldExpr));
end;

{$ENDIF}

function TStreamOperation<T>.GroupBy<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>;
begin
  exit(Enum.GroupBy<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction));
end;

function TStreamOperation<T>.GroupBy<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, T>;
begin
  exit(Enum.GroupBy<T, TKeyType>(FEnum, AField.FieldExpr));
end;

function TStreamOperation<T>.GroupToArray<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>;
begin
  exit(Enum.GroupToArray<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction));
end;

function TStreamOperation<T>.GroupToArray<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, TArray<T>>;
begin
  exit(Enum.GroupToArray<T, TKeyType>(FEnum, AField.FieldExpr));
end;

function TStreamOperation<T>.GroupToLists<TKeyType, TValueType>(AField: TFieldExpression; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>;
begin
  exit(Enum.GroupToLists<T, TKeyType, TValueType>(FEnum, AField.FieldExpr, AFunction));
end;

function TStreamOperation<T>.GroupToLists<TKeyType>(AField: TFieldExpression): TDictionary<TKeyType, TList<T>>;
begin
  exit(Enum.GroupToLists<T, TKeyType>(FEnum, AField.FieldExpr));
end;

function TStreamOperation<T>.IsCached: boolean;
begin
  exit(supports(FEnum, IEnumCache<T>));
end;

class operator TStreamOperation<T>.Implicit(const [ref] OP: TStreamOperation<T>): IEnum<T>;
begin
  exit(OP.FEnum);
end;

function TStreamOperation<T>.InnerJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  exit(TJoinEnum<T, TOther, TJoined>.Create( //
    FEnum, AOther.FEnum, AOn, ASelect));
end;

function TStreamOperation<T>.LeftJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  exit(TLeftJoinEnum<T, TOther, TJoined>.Create(FEnum, AOther.FEnum, AOn, ASelect));
end;

function TStreamOperation<T>.Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TStreamOperation<TOutput>;
begin
  exit(TMapEnum<T, TOutput>.Create(FEnum, AFunction));
end;

function TStreamOperation<T>.Max(AComparer: IComparer<T>): T;
begin
  exit(Enum.Max<T>(FEnum, AComparer));
end;
{
  function TStreamOperation<T>.Max(const AComparer: TComparer<T>): T;
  begin
  exit(Enum.Max<T>(FEnum, AComparer));
  end; }

function TStreamOperation<T>.Max: T;
begin
  exit(Enum.Min<T>(FEnum));
end;

function TStreamOperation<T>.Min: T;
begin
  exit(Enum.Min<T>(FEnum));
end;

function TStreamOperation<T>.Min(AComparer: IComparer<T>): T;
begin
  exit(Enum.Min<T>(FEnum, AComparer));
end;

{
  function TStreamOperation<T>.Min(const AComparer: TComparer<T>): T;
  begin
  exit(Enum.Min<T>(FEnum, AComparer));
  end;
}
function TStreamOperation<T>.Reverse: TStreamOperation<T>;
begin
  exit(Enum.Reverse<T>(FEnum));
end;

function TStreamOperation<T>.RightJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  exit(AOther.LeftJoin<T, TJoined>(Self,
    function(const A: TOther; const B: T): boolean
    begin
      exit(AOn(B, A));
    end,
    function(const A: TOther; const B: T): TJoined
    begin
      exit(ASelect(B, A));
    end));
end;

function TStreamOperation<T>.Sort(const ADirection: TSortOrder; AComparator: IComparer<T>): TStreamOperation<T>;
var
  Comparer: IComparer<T>;
begin
  if AComparator = nil then
    Comparer := System.Generics.Defaults.TComparer<T>.Default
  else
    Comparer := AComparator;
  if ADirection = soDescending then
    Comparer := TReverseComparer<T>.Create(Comparer);
  exit(Sort(Comparer));
end;

function TStreamOperation<T>.Sort(AComparator: IComparer<T>): TStreamOperation<T>;
var
  RttiType: TRttiType;
  Comparer: IComparer<T>;
begin
  RttiType := RttiCtx.GetType(typeinfo(T));
  if (RttiType.TypeKind in [tkClass, tkRecord]) then
  begin
    raise EStream.Create('SortBy should be used on primitive types only');
  end
  else
  begin
    if AComparator = nil then
      Comparer := System.Generics.Defaults.TComparer<T>.Default
    else
      Comparer := AComparator;
    exit(TSortedEnum<T>.Create(FEnum, Comparer));
  end;
end;

function TStreamOperation<T>.SortBy(const AExpr: TSortExpression): TStreamOperation<T>;
var
  RttiType: TRttiType;
begin
  RttiType := RttiCtx.GetType(typeinfo(T));
  if (RttiType.TypeKind in [tkClass, tkRecord]) then
  begin
    exit(TSortedEnum<T>.Create(FEnum, //
      TClassOrRecordComparer<T>.Create(AExpr.Exprs)));
  end
  else
  begin
    raise EStream.Create('SortBy should be used on classes or records only');

    exit(TSortedEnum<T>.Create(FEnum, //
      System.Generics.Defaults.TComparer<T>.Default));
  end;
end;

function TStreamOperation<T>.Schuffle: TStreamOperation<T>;
begin
  exit(Enum.Schuffle<T>(FEnum));
end;

function TStreamOperation<T>.Skip(const ANumber: integer): TStreamOperation<T>;
begin
  exit(TSkip<T>.Create(FEnum, ANumber));
end;

function TStreamOperation<T>.Take(const ANumber: integer): TStreamOperation<T>;
begin
  exit(TTake<T>.Create(FEnum, ANumber));
end;

function TStreamOperation<T>.TakeOne: T;
var
  res: TArray<T>;
begin
  res := Enum.ToArray<T>(TTake<T>.Create(FEnum, 1));
  if length(res) <> 1 then
    raise EStreamItemNotFound.Create('Expecting one item');
  exit(res[0]);
end;

function TStreamOperation<T>.ToArray: TArray<T>;
begin
  exit(Enum.ToArray<T>(FEnum));
end;

function TStreamOperation<T>.ToList: TList<T>;
begin
  exit(Enum.ToList<T>(FEnum));
end;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}

function TStreamOperation<T>.ToIList(): IList<T>;
begin
  result := TCollections.CreateList<T>();
  Enum.CopyFromEnum<T>(FEnum, result);
end;

function TStreamOperation<T>.ToISet(): ISet<T>;
begin
  result := TCollections.CreateSet<T>();
  Enum.CopyFromEnum<T>(FEnum, result);
end;

{$ENDIF}

function TStreamOperation<T>.TryTakeOne(out AValue: T): boolean;
var
  res: TArray<T>;
begin
  res := Enum.ToArray<T>(TTake<T>.Create(FEnum, 1));
  if length(res) <> 1 then
    exit(false);
  AValue := res[0];
  res := nil;
  exit(true);
end;

function TStreamOperation<T>.Union(const [ref] AOther: TStreamOperation<T>): TStreamOperation<T>;
begin
  exit(TUnionEnum<T>.Create([Self.FEnum, AOther.FEnum]));
end;

function TStreamOperation<T>.Unique(AComparator: IComparer<T>): TStreamOperation<T>;
begin
  exit(TUniqueEnum<T>.Create(FEnum, AComparator));
end;

procedure TStreamOperation<T>.Update(const AFunction: TApplyProc<T>);
begin
  Apply(AFunction);
end;

function TStreamOperation<T>.Where(const ACondition: TFilterFunction<T>): TStreamOperation<T>;
begin
  exit(Filter(ACondition));
end;

function TStreamOperation<T>.Where(const ACondition: TExpression): TStreamOperation<T>;
begin
  exit(Filter(ACondition));
end;

function TStreamOperation<T>.Unique: TStreamOperation<T>;
begin
  exit(TUniqueEnum<T>.Create(FEnum, System.Generics.Defaults.TComparer<T>.Default)); //
end;

function TStreamOperation<T>.All(const APredicate: TPredicate<T>): boolean;
begin
  exit(Enum.All<T>(FEnum, APredicate));
end;

function TStreamOperation<T>.Any(const APredicate: TPredicate<T>): boolean;
begin
  exit(Enum.Any<T>(FEnum, APredicate));
end;

procedure TStreamOperation<T>.Apply(const AFunction: TApplyProc<T>);
begin
  Enum.Apply<T>(FEnum, AFunction);
end;

function TStreamOperation<T>.Filter(const ACondition: TExpression): TStreamOperation<T>;
begin
  exit(TFilterEnum<T>.Create(FEnum, //
    TExprFilter<T>.Create(ACondition.Expr) //
    ));
end;

function TStreamOperation<T>.Filter(const ACondition: TFilterFunction<T>): TStreamOperation<T>;
begin
  exit(TFilterEnum<T>.Create(FEnum, //
    TTypedFunctionFilter<T>.Create(ACondition) //
    ));
end;

function TStreamOperation<T>.FullJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
var
  leftCache: TStreamOperation<T>;
  rightCache: TStreamOperation<TOther>;
begin
  leftCache := Enum.Cache<T>(FEnum);
  rightCache := Enum.Cache<TOther>(AOther.FEnum);
  exit(TUnionEnum<TJoined>.Create( //
    [leftCache.LeftJoin<TOther, TJoined>(rightCache, AOn, ASelect).FEnum, //
    leftCache.RightJoin<TOther, TJoined>(rightCache, AOn, ASelect).FEnum //
    ]));
end;

{ Stream }

class function Stream.From<T>(const ASource: TDataSet): TStreamOperation<T>;
var
  RttiType: TRttiType;
begin
  RttiType := RttiCtx.GetType(typeinfo(T));
  case RttiType.TypeKind of
    tkClass:
      exit(TDataSetEnumClass<T>.Create(ASource));
    tkRecord:
      exit(TDataSetEnumRecord<T>.Create(ASource));
  else
    raise EStreamReflect.Create('Type must a a record or a class');
  end;
end;

class function Stream.From(const ASource: string): TStreamOperation<char>;
begin
  exit(TStringEnum.Create(ASource));
end;

class function Stream.From<T>(const ASource: TArray<T>): TStreamOperation<T>;
begin
  exit(TArrayEnum<T>.Create(ASource));
end;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}

class function Stream.From<T>(ASource: Spring.Collections.IEnumerable<T>): TStreamOperation<T>;
begin
  exit(Enum.FromSpring4D<T>(ASource));
end;

{$ENDIF}

class function Stream.From<T>(const ASource: TEnumerable<T>): TStreamOperation<T>;
begin
  exit(TTEnumerableEnum<T>.Create(ASource));
end;

class function Stream.Range(const AStart, AEnd, ADelta: extended): TStreamOperation<extended>;
begin
  exit(TFloatRangeEnum.Create(AStart, AEnd, ADelta));
end;

class function Stream.Range(const AStart, AEnd, ADelta: int64): TStreamOperation<int64>;
begin
  exit(TIntRangeEnum.Create(AStart, AEnd, ADelta));
end;

class function Stream.ReflectMetadata<TMetadata, T>: TMetadata;
var
  attrib: TCustomAttribute;
  metaType, otherType, fieldExprType: TRttiType;
  fld: TRttiField;
  name: string;
  Expr: TFieldExpression;
  exprVal, resVal, v: TValue;
begin
  fillchar(result, sizeof(result), 0);
  fieldExprType := RttiCtx.GetType(typeinfo(TFieldExpression));
  metaType := RttiCtx.GetType(typeinfo(TMetadata));
  resVal := TValue.From<TMetadata>(result);

  // resolve otherType by searching for StreamClass on metaType
  otherType := RttiCtx.GetType(typeinfo(T));;

  // ensure all fields on metadata class are of type TFieldExpression
  // and validate the names
  for fld in metaType.GetFields do
  begin
    name := fld.name;
    if fld.FieldType <> fieldExprType then
      raise EStreamReflect.CreateFmt('Metadata field ''%s'' should be ''%s''', [name, fieldExprType.name]);

    for attrib in fld.GetAttributes do
    begin
      if attrib is StreamFieldAttribute then
      begin
        name := StreamFieldAttribute(attrib).name;
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

class function Stream.From<T>(ASource: System.IEnumerable<T>): TStreamOperation<T>;
begin
  exit(TIEnumerableEnum<T>.Create(ASource));
end;

{ StreamFieldAttribute }

constructor StreamFieldAttribute.Create(const AName: string);
begin
  FName := AName;
end;

type
  TFieldExtractor = class(TInterfacedObject, IFieldExtractor)
  private
    FRttiField: TArray<TRttiField>;
    function GetRttiFields: TArray<TRttiField>;
    function GetRttiType: TRttiType;
  public
    constructor Create(const AFields: TArray<TRttiField>);
    function GetValue(const AValue: TValue; var Value: TValue): boolean;
    property RttiFields: TArray<TRttiField> read GetRttiFields;
    property RttiType: TRttiType read GetRttiType;
  end;

function GetInterfaceTypeInfo(InterfaceTable: PInterfaceTable): ptypeinfo;
var
  P: PPointer;
begin
  if Assigned(InterfaceTable) and (InterfaceTable^.EntryCount > 0) then
  begin
    P := Pointer(NativeUInt(@InterfaceTable^.Entries[InterfaceTable^.EntryCount]));
    exit(Pointer(NativeUInt(P^) + sizeof(Pointer)));
  end
  else
    exit(nil);
end;

{ TStreamTypeCache }

constructor TStreamTypeCache.Create;
begin
  FLock := TCriticalSection.Create;
  FMethods := TDictionary<ptypeinfo, TRttiInvokableType>.Create;
  FTypes := TDictionary<ptypeinfo, TRttiType>.Create;
  FExtractors := TDictionary<TArray<TRttiField>, IFieldExtractor>.Create;
end;

destructor TStreamTypeCache.Destroy;
begin
  FLock.Free;
  FExtractors.Clear;
  FExtractors.Free;
  FMethods.Free;
  FTypes.Free;
  inherited;
end;

function TStreamTypeCache.GetExtractor(const A: TArray<TRttiField>): IFieldExtractor;
begin
  result := nil;
  FLock.Acquire;
  try
    if not FExtractors.TryGetValue(A, result) then
    begin
      result := TFieldExtractor.Create(A);
      FExtractors.Add(A, result);
    end;
  finally
    FLock.Release;
  end;
end;

function GetFieldsFromString(const AType: TRttiType; const A: string): TArray<TRttiField>;
var
  parts: TArray<string>;
  f: TRttiField;
  I: integer;
  numparts: integer;
begin
  parts := A.trim.Split(['.']);
  numparts := length(parts);
  f := AType.GetField(parts[0]);
  setlength(result, 1);
  result[0] := f;
  for I := 1 to numparts - 1 do
  begin
    f := f.FieldType.GetField(parts[I]);
    if f = nil then
      raise EStream.Create('field not found');
    insert(f, result, length(result));
  end;
end;

function TStreamTypeCache.GetExtractor(const AType: TRttiType; const A: string): IFieldExtractor;
begin
  result := GetExtractor(GetFieldsFromString(AType, A));
end;

function TStreamTypeCache.GetMethod(const AInfo: ptypeinfo): TRttiInvokableType;
begin
  result := nil;
  FLock.Acquire;
  try
    if not FMethods.TryGetValue(AInfo, result) then
    begin
      result := RttiCtx.GetType(AInfo) as TRttiInvokableType;
      FMethods.Add(AInfo, result);
    end;
  finally
    FLock.Release;
  end;
end;

function TStreamTypeCache.GetType(const AInfo: ptypeinfo): TRttiType;
begin
  result := nil;
  FLock.Acquire;
  try
    if not FTypes.TryGetValue(AInfo, result) then
    begin
      result := RttiCtx.GetType(AInfo);
      FTypes.Add(AInfo, result);
    end;
  finally
    FLock.Release;
  end;
end;

{ TFieldExtractor }

constructor TFieldExtractor.Create(const AFields: TArray<TRttiField>);
begin
  if length(AFields) = 0 then
    raise EStream.Create('fields expected');
  FRttiField := AFields;
end;

function TFieldExtractor.GetRttiFields: TArray<TRttiField>;
begin
  result := FRttiField;
end;

function TFieldExtractor.GetRttiType: TRttiType;
begin
  result := FRttiField[high(FRttiField)].FieldType;
end;

function TFieldExtractor.GetValue(const AValue: TValue; var Value: TValue): boolean;
var
  f: TRttiField;
  o: TObject;
begin
  Value := AValue;
  for f in FRttiField do
  begin
    case Value.Kind of
      tkRecord:
        begin
          Value := f.GetValue(Value.GetReferenceToRawData);
          exit(true);
        end;
      tkClass:
        begin
          o := Value.AsObject;
          if o = nil then
            exit(false);
          Value := f.GetValue(o);
          exit(true);
        end
    else
      exit(true);
    end;
  end;
  exit(false);
end;

class function TObjectHelper.SupportsInterface<T>(out Intf: T): boolean;
var
  intfTable: PInterfaceTable;
  IntfTypeInfo: ptypeinfo;
  I: integer;
begin
  intfTable := GetInterfaceTable;
  IntfTypeInfo := GetInterfaceTypeInfo(intfTable);
  for I := 0 to intfTable^.EntryCount - 1 do
  begin
    if IntfTypeInfo = typeinfo(T) then
      exit(true);
    inc(IntfTypeInfo);
  end;
  exit(false);
end;

class function TObjectHelper.SupportsInterface<TC, T>(const AClass: TC): boolean;
var
  intfTable: PInterfaceTable;
  IntfTypeInfo: ptypeinfo;
  I: integer;
begin
  intfTable := AClass.GetInterfaceTable;
  IntfTypeInfo := GetInterfaceTypeInfo(intfTable);
  for I := 0 to intfTable^.EntryCount - 1 do
  begin
    if IntfTypeInfo = typeinfo(T) then
      exit(true);
    inc(IntfTypeInfo);
  end;
  exit(false);
end;

{ TSortExpr }

constructor TSortExpr.Create(const AName: string; const AOrder: TSortOrder);
begin
  FName := AName;
  FOrder := AOrder;
end;

function TSortExpr.GetField: TRttiField;
begin
  exit(FField);
end;

function TSortExpr.GetName: string;
begin
  exit(FName);
end;

function TSortExpr.GetOrder: TSortOrder;
begin
  exit(FOrder);
end;

procedure TSortExpr.SetField(const Value: TRttiField);
begin
  FField := Value;
end;

{ TStringComparer }

function TStringComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: string;
begin
  avs := A.asstring;
  bvs := B.asstring;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TIntegerComparer }

function TIntegerComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: int64;
begin
  avs := A.AsInt64;
  bvs := B.AsInt64;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TDoubleComparer }

function TDoubleComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: double;
begin
  avs := A.AsExtended;
  bvs := B.AsExtended;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TBooleanComparer }

function TBooleanComparer.Compare(const A, B: TValue): integer;
var
  avs, bvs: boolean;
begin
  avs := A.AsBoolean;
  bvs := B.AsBoolean;
  if avs < bvs then
    exit(-1)
  else if avs = bvs then
    exit(0)
  else
    exit(1);
end;

{ TClassOrRecordComparer<T> }

function TClassOrRecordComparer<T>.Compare(const A, B: T): integer;
var
  I, cv: integer;
  av, bv: TValue;
  e: IFieldExtractor;
begin
  for I := 0 to length(FExtractors) - 1 do
  begin
    e := FExtractors[I];
    if not e.GetValue(TValue.From<T>(A), av) then
      exit(1);
    if not e.GetValue(TValue.From<T>(B), bv) then
      exit(-1);
    cv := FComparators[I].Compare(av, bv);
    if cv = 0 then
      continue;
    if FExprs[I].Order = soDescending then
      cv := -cv;
    exit(cv);
  end;
  exit(0);
end;

constructor TClassOrRecordComparer<T>.Create(AComparators: TArray<IComparer<TValue>>; AExprs: TArray<ISortExpr>; AExtractors: TArray<IFieldExtractor>);
begin
  FComparators := AComparators;
  FExtractors := AExtractors;
  FExprs := AExprs;
end;

constructor TClassOrRecordComparer<T>.Create(AExprs: TArray<ISortExpr>);
var
  RttiType: TRttiType;
  Comparer: IComparer<TValue>;
  comparators: TArray<IComparer<TValue>>;
  extractors: TArray<IFieldExtractor>;
  e: IFieldExtractor;
  Field: ISortExpr;
  I: integer;
begin
  RttiType := RttiCtx.GetType(typeinfo(T));
  setlength(comparators, 0);
  setlength(extractors, 0);
  for I := 0 to length(AExprs) - 1 do
  begin
    Field := AExprs[I];
    e := StreamCache.GetExtractor(RttiType, Field.name);
    insert(e, extractors, length(extractors));
    case e.RttiType.TypeKind of
      tkInteger, tkInt64:
        Comparer := SortInt64;
      tkEnumeration:
        Comparer := SortBoolean;
      tkString, tkAnsiString, tkWideString, tkUnicodeString:
        Comparer := SortString;
      tkFloat:
        Comparer := SortDouble;
    else
      raise EStream.Create('type not supported');
    end;
    insert(Comparer, comparators, length(comparators));
  end;
  if length(AExprs) = 0 then
    raise EStream.Create('sort expressions expected');

  Create(comparators, AExprs, extractors)
end;

destructor TClassOrRecordComparer<T>.Destroy;
begin
  FComparators := nil;
  FExtractors := nil;
  FExprs := nil;
  inherited;
end;

{ TReverseComparer<T> }

function TReverseComparer<T>.Compare(const Left, Right: T): integer;
begin
  exit(-FComparer.Compare(Left, Right));
end;

constructor TReverseComparer<T>.Create(Comparer: IComparer<T>);
begin
  FComparer := Comparer;
end;

destructor TReverseComparer<T>.Destroy;
begin
  FComparer := nil;
  inherited;
end;

{ TExprVisitor }

procedure TExprVisitor.Visit(const AExpr: TUnaryExpr);
begin
end;

procedure TExprVisitor.Visit(const AExpr: TBinaryExpr);
begin
end;

procedure TExprVisitor.Visit(const AExpr: TFieldExpr);
begin
end;

procedure TExprVisitor.Visit(const AExpr: TBoolExpr);
begin
end;

{ TRttiExprVisitor }

constructor TRttiExprVisitor.Create(const AType: TRttiType);
begin
  FType := AType;
end;

procedure TRttiExprVisitor.Visit(const AExpr: TFieldExpr);
begin
  AExpr.RttiField := StreamCache.GetExtractor(FType, AExpr.Field);
  if AExpr.Value.typeinfo = typeinfo(TFieldExpr) then
    Visit(AExpr.Value.AsType<TFieldExpr>);
end;

{ TFieldExpr }

constructor TFieldExpr.Create(const AField: string);
begin
  FField := AField.trim();
end;

function TFieldExpr.IsTrue(const [ref] AValue: TValue): boolean;

  function GetValue(out v: TValue): boolean;
  begin
    exit(RttiField.GetValue(AValue, v));
  end;

  function GetRightValue(out v: TValue): boolean;
  begin
    v := FValue;
    if FValue.typeinfo = typeinfo(TFieldExpr) then
      exit(TFieldExpr(FValue.AsObject).RttiField.GetValue(AValue, v))
    else
      exit(true);
  end;

  function RaiseOperatorNotSupported: boolean;
  begin
    raise TExprException.Create('operator not supported');
  end;

  function FilterBoolean(const [ref] A, B: TValue): boolean;

    function GetVal(const A: TValue): boolean;
    begin
      exit(A.AsBoolean);
    end;

  var
    ab, bb: boolean;

  begin
    ab := GetVal(A);
    bb := GetVal(B);
    case FOP of
      foEQ:
        exit(ab = bb);
      foNEQ:
        exit(ab <> bb);
      foLT:
        exit(ab < bb);
      foLTE:
        exit(ab <= bb);
      foGT:
        exit(ab > bb);
      foGTE:
        exit(ab >= bb);
    else
      exit(RaiseOperatorNotSupported);
    end;
  end;

  function AsInt64(const [ref] AValue: TValue): int64;
  begin
    case AValue.Kind of
      tkInteger, tkInt64:
        exit(AValue.AsInt64);
      tkFloat:
        exit(trunc(AValue.AsExtended));
    else
      exit(0);
    end;
  end;

  function FilterInt(const [ref] A, B: TValue): boolean;

    function GetVal(const A: TValue): int64;
    begin
      exit(A.AsInt64);
    end;

  var
    ab, bb: int64;

  begin
    ab := GetVal(A);
    bb := AsInt64(B);
    case FOP of
      foEQ:
        exit(ab = bb);
      foNEQ:
        exit(ab <> bb);
      foLT:
        exit(ab < bb);
      foLTE:
        exit(ab <= bb);
      foGT:
        exit(ab > bb);
      foGTE:
        exit(ab >= bb);
    else
      exit(RaiseOperatorNotSupported);
    end;
  end;

  function AsFloat(const [ref] AValue: TValue): double;
  begin
    case AValue.Kind of
      tkInteger, tkInt64:
        exit(AValue.AsInt64);
      tkFloat:
        exit(AValue.AsExtended);
    else
      exit(0);
    end;
  end;

  function FilterFloat(const [ref] A, B: TValue): boolean;
    function GetVal(const A: TValue): double;
    begin
      exit(A.AsExtended);
    end;

  var
    ab, bb: double;

  begin
    ab := GetVal(A);
    bb := AsFloat(B);
    case FOP of
      foEQ:
        exit(ab = bb);
      foNEQ:
        exit(ab <> bb);
      foLT:
        exit(ab < bb);
      foLTE:
        exit(ab <= bb);
      foGT:
        exit(ab > bb);
      foGTE:
        exit(ab >= bb);
    else
      exit(RaiseOperatorNotSupported);
    end;
  end;

  function FilterString(const [ref] A, B: TValue): boolean;
    function GetVal(const A: TValue): string;
    begin
      exit(A.asstring);
    end;

  var
    ab, bb: string;
  begin
    ab := GetVal(A);
    bb := GetVal(B);
    case FOP of
      foEQ:
        exit(ab = bb);
      foNEQ:
        exit(ab <> bb);
      foLT:
        exit(ab < bb);
      foLTE:
        exit(ab <= bb);
      foGT:
        exit(ab > bb);
      foGTE:
        exit(ab >= bb);
    else
      exit(RaiseOperatorNotSupported);
    end;
  end;

var
  r, v: TValue;

begin
  if not GetValue(v) then
    exit(false);
  if not GetRightValue(r) then
    exit(false);
  case v.Kind of
    tkEnumeration:
      if typeinfo(boolean) = v.typeinfo then
        exit(FilterBoolean(v, r))
      else
        raise TExprException.Create('enum not supported');
    tkInteger, tkInt64:
      exit(FilterInt(v, r));
    tkFloat:
      exit(FilterFloat(v, r));
    tkString, tkWideString, tkUnicodeString, tkAnsiString:
      exit(FilterString(v, r));
  else
    raise TExprException.Create('field type not supported');
  end;
end;

procedure TFieldExpr.SetOP(const Value: TFieldExprOper);
begin
  Self.FOP := Value;
end;

procedure TFieldExpr.SetRttiField(const Value: IFieldExtractor);
begin
  FRttiField := Value;
end;

procedure TFieldExpr.SetValue(const Value: TValue);
begin
  FValue := Value;
end;

destructor TFieldExpr.Destroy;
begin
  FRttiField := nil;
  inherited;
end;

function TFieldExpr.GetExprType: TExprType;
begin
  exit(etField);
end;

function TFieldExpr.GetField: string;
begin
  exit(FField);
end;

function TFieldExpr.GetOP: TFieldExprOper;
begin
  exit(FOP);
end;

function TFieldExpr.GetRttiField: IFieldExtractor;
begin
  exit(FRttiField);
end;

function TFieldExpr.GetValue: TValue;
begin
  exit(FValue);
end;

{ TBinOp }

procedure TBinaryExpr.Accept(const AVisitor: TExprVisitor);
var
  e: IVisitableExpr;
begin
  if supports(FLeft, IVisitableExpr, e) then
    e.Accept(AVisitor);
  if supports(FRight, IVisitableExpr, e) then
    e.Accept(AVisitor);
end;

constructor TBinaryExpr.Create(ALeft: IExpr; const AOP: TOper; ARight: IExpr);
begin
  FLeft := ALeft;
  FOP := AOP;
  FRight := ARight;
end;

destructor TBinaryExpr.Destroy;
begin
  FLeft := nil;
  FRight := nil;
  inherited;
end;

function TBinaryExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  result := FLeft.IsTrue(AValue);
  case FOP of
    boAND:
      exit(result and FRight.IsTrue(AValue));
    boOR:
      exit(result or FRight.IsTrue(AValue));
  end;
end;

function TBinaryExpr.GetExprType: TExprType;
begin
  exit(etBinary);
end;

{ TUnaryOp }

procedure TUnaryExpr.Accept(const AVisitor: TExprVisitor);
var
  e: IVisitableExpr;
begin
  if supports(FExpr, IVisitableExpr, e) then
    e.Accept(AVisitor);
end;

constructor TUnaryExpr.Create(AExpr: IExpr; const AOP: TOper);
begin
  FExpr := AExpr;
  FOP := AOP;
end;

destructor TUnaryExpr.Destroy;
begin
  FExpr := nil;
  inherited;
end;

function TUnaryExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  case FOP of
    uoNOT:
      exit(not FExpr.IsTrue(AValue));
  else
    raise TExprException.Create('unary type not supported');
  end;
end;

function TUnaryExpr.GetExprType: TExprType;
begin
  exit(etUnary);
end;

{ TExpr }

procedure TExpr.Accept(const AVisitor: TExprVisitor);
begin
  if Self is TFieldExpr then
    AVisitor.Visit(AsFieldExpr)
  else if Self is TBinaryExpr then
    AVisitor.Visit(AsBinaryExpr)
  else if Self is TUnaryExpr then
    AVisitor.Visit(AsUnaryExpr)
  else if Self is TBoolExpr then
    AVisitor.Visit(AsBoolExpr)
  else
    raise TExprException.Create('unexpected expression type');
end;

function TExpr.AsBinaryExpr: TBinaryExpr;
begin
  exit(Self as TBinaryExpr);
end;

function TExpr.AsBoolExpr: TBoolExpr;
begin
  exit(Self as TBoolExpr);
end;

function TExpr.AsFieldExpr: TFieldExpr;
begin
  exit(Self as TFieldExpr);
end;

function TExpr.AsUnaryExpr: TUnaryExpr;
begin
  exit(Self as TUnaryExpr);
end;

function TExpr.IsExprType(const AExprType: TExprType): boolean;
begin
  exit(GetExprType = AExprType);
end;

{ TBoolExpr }

constructor TBoolExpr.Create(const AValue: boolean);
begin
  FValue := AValue;
end;

function TBoolExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  exit(FValue);
end;

function TBoolExpr.GetExprType: TExprType;
begin
  exit(etBoolean);
end;

{ TFilterExpr }

constructor TFilterExpr.Create(Expr: IFilterFunction);
begin
  FExpr := Expr;
end;

function TFilterExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  exit(FExpr.IsTrue(AValue));
end;

destructor TFilterExpr.Destroy;
begin
  FExpr := nil;
  inherited;
end;

function TFilterExpr.GetExprType: TExprType;
begin
  exit(etFilter);
end;

{ TExprFilter<T> }

constructor TExprFilter<T>.Create(AExpr: IExpr);
var
  visitor: TRttiExprVisitor;
  e: IVisitableExpr;
begin
  visitor := TRttiExprVisitor.Create(RttiCtx.GetType(typeinfo(T)));
  try
    if supports(AExpr, IVisitableExpr, e) then
      e.Accept(visitor);
  finally
    visitor.Free;
  end;
  FExpr := AExpr;
end;

destructor TExprFilter<T>.Destroy;
begin
  FExpr := nil;
  inherited;
end;

function TExprFilter<T>.IsTrue(const AData: TValue): boolean;
begin
  exit(FExpr.IsTrue(AData));
end;

{ TTypedFunctionFilter<T> }

constructor TTypedFunctionFilter<T>.Create(const AFunction: TFilterFunction<T>);
begin
  FFunction := AFunction;
end;

function TTypedFunctionFilter<T>.IsTrue(const AData: TValue): boolean;
begin
  exit(FFunction(AData.AsType<T>()));
end;

{ TFilterProcessor<T> }

function TAbstractFilter<T>.IsTrue(const AData: T): boolean;
begin
  exit(IsTrue(TValue.From<T>(AData)));
end;

{ TArrayEnum<T> }

constructor TArrayEnum<T>.Create(const AData: TArray<T>);
begin
  inherited Create();
  FData := AData;
end;

function TArrayEnum<T>.Current: T;
begin
  exit(FData[FOffset]);
end;

function TArrayEnum<T>.EOF: boolean;
begin
  exit(FOffset = length(FData));
end;

function TArrayEnum<T>.GetCache: TList<T>;
begin
  exit(Enum.ToList<T>(TArrayEnum<T>.Create(FData)));
end;

function TArrayEnum<T>.GetEnum: IEnum<T>;
begin
  exit(TArrayEnum<T>.Create(FData));
end;

procedure TArrayEnum<T>.Next;
begin
  if EOF then
    exit;
  inc(FOffset);
end;

{ TIEnumerableEnum<T> }

constructor TIEnumerableEnum<T>.Create(const AEnum: System.IEnumerable<T>);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TIEnumerableEnum<T>.Current: T;
begin
  exit(FEnum.Current);
end;

destructor TIEnumerableEnum<T>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TIEnumerableEnum<T>.EOF: boolean;
begin
  exit(FEof);
end;

procedure TIEnumerableEnum<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}
{ TSpringIEnumerableEnum<T> }

constructor TSpringIEnumerableEnum<T>.Create(const AEnum: Spring.Collections.IEnumerable<T>);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  Next;
end;

function TSpringIEnumerableEnum<T>.Current: T;
begin
  exit(FEnum.Current);
end;

destructor TSpringIEnumerableEnum<T>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TSpringIEnumerableEnum<T>.EOF: boolean;
begin
  exit(FEof);
end;

procedure TSpringIEnumerableEnum<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;
{$ENDIF}
{ TBaseEnum<T> }

constructor TBaseEnum<T>.Create(AEnum: IEnum<T>);
begin
  inherited Create();
  if not Enum.TryGetCached<T>(AEnum, FEnum) then
    FEnum := AEnum;
end;

function TBaseEnum<T>.Current: T;
begin
  exit(FEnum.Current);
end;

destructor TBaseEnum<T>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TBaseEnum<T>.EOF: boolean;
begin
  exit(FEnum.EOF);
end;

procedure TBaseEnum<T>.Next;
begin
  FEnum.Next;
end;

{ TFilterEnum<T> }

constructor TFilterEnum<T>.Create(AEnum: IEnum<T>; const AFilter: IFilterFunction<T>);
begin
  inherited Create(AEnum);
  FFilter := AFilter;
  Next;
end;

constructor TFilterEnum<T>.Create(AEnum: IEnum<T>; const AFilter: TFilterFunction<T>);
begin
  Create(AEnum, TTypedFunctionFilter<T>.Create(AFilter));
end;

function TFilterEnum<T>.Current: T;
begin
  exit(FNext);
end;

destructor TFilterEnum<T>.Destroy;
begin
  FFilter := nil;
  inherited;
end;

function TFilterEnum<T>.EOF: boolean;
begin
  exit(FEnum.EOF and not FHasValue);
end;

procedure TFilterEnum<T>.Next;
begin
  FHasValue := false;
  if FEnum.EOF then
    exit;
  while not FEnum.EOF do
  begin
    FNext := FEnum.Current;
    FEnum.Next;
    if FFilter.IsTrue(FNext) then
    begin
      FHasValue := true;
      break;
    end;
  end;
end;

{ FSkip<T> }

constructor TSkip<T>.Create(AEnum: IEnum<T>; const ASkip: integer);
var
  Skip: integer;
begin
  inherited Create(AEnum);
  Skip := ASkip;
  while (Skip > 0) and not EOF do
  begin
    Next;
    dec(Skip);
  end;
end;

{ TTake<T> }

constructor TTake<T>.Create(AEnum: IEnum<T>; const ATake: integer);
begin
  inherited Create(AEnum);
  FTake := ATake;
end;

function TTake<T>.EOF: boolean;
begin
  exit(FEnum.EOF or FEof);
end;

procedure TTake<T>.Next;
begin
  FEnum.Next;
  if FTake > 0 then
    dec(FTake);
  if FTake = 0 then
    FEof := true;
end;

{ TMap<TInput, TOutput> }

constructor TMapEnum<TInput, TOutput>.Create(AEnum: IEnum<TInput>; AMapper: TMapFunction<TInput, TOutput>);
begin
  inherited Create();
  if not Enum.TryGetCached<TInput>(AEnum, FEnum) then
    FEnum := AEnum;
  FMapper := AMapper;
end;

function TMapEnum<TInput, TOutput>.Current: TOutput;
begin
  exit(FMapper(FEnum.Current));
end;

destructor TMapEnum<TInput, TOutput>.Destroy;
begin
  FEnum := nil;
  inherited;
end;

function TMapEnum<TInput, TOutput>.EOF: boolean;
begin
  exit(FEnum.EOF);
end;

procedure TMapEnum<TInput, TOutput>.Next;
begin
  FEnum.Next;
end;

{ TEnumCache<T> }

constructor TEnumCache<T>.Create(AEnum: IEnum<T>; const AOwn: boolean);
begin
  inherited Create();
  FCache := TList<T>.Create;
  FOwn := AOwn;
  while AEnum.HasMore do
  begin
    FCache.Add(AEnum.Current);
  end;
end;

constructor TEnumCache<T>.Create(AEnum: TList<T>);
begin
  inherited Create();
  FCache := AEnum;
  FOwn := false;
end;

constructor TEnumCache<T>.Create(AEnum: TEnumerable<T>);
var
  e: TEnumerator<T>;
begin
  inherited Create();
  FCache := TList<T>.Create;
  FOwn := false;
  e := AEnum.GetEnumerator;
  while e.MoveNext do
  begin
    FCache.Add(e.Current);
  end;
end;

destructor TEnumCache<T>.Destroy;
var
  val: T;
  obj: TObject;
begin
  if FOwn then
  begin
    for val in FCache do
    begin
      move(val, obj, sizeof(obj));
      obj.Free;
    end;
  end;
  FCache.Free;
  inherited;
end;

function TEnumCache<T>.GetCache: TList<T>;
begin
  exit(FCache);
end;

function TEnumCache<T>.GetEnum: IEnum<T>;
begin
  exit(TCachedEnum<T>.Create(Self));
end;

{ TApplyEnum<T> }

constructor TApplyEnum<T>.Create(AEnum: IEnum<T>; const AApply: TApplyProc<T>);
begin
  inherited Create(AEnum);
  FApply := AApply;
end;

function TApplyEnum<T>.Current: T;
begin
  result := FEnum.Current;
  FApply(result);
end;

{ THasMore<T> }

constructor THasMore<T>.Create;
begin
  FFirst := false;
end;

function THasMore<T>.HasMore: boolean;
begin
  if FFirst then
    Next;
  FFirst := true;
  exit(not EOF);
end;

{ Enum }

class function Enum.ToArray<T>(AEnum: IEnum<T>): TArray<T>;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  setlength(result, 0);
  while e.HasMore do
    insert(e.Current, result, length(result));
end;

class function Enum.ToList<T>(AEnum: IEnum<T>): TList<T>;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TList<T>.Create;
  while e.HasMore do
    result.Add(e.Current);
end;

class function Enum.TryGetCached<T>(AEnum: IEnum<T>; out ACachedEnum: IEnum<T>): boolean;
var
  c: TCachedEnum<T>;
begin
  if Enum.IsCached<T>(AEnum) then
  begin
    c := AEnum as TCachedEnum<T>;
    ACachedEnum := c.GetEnum;
    exit(true);
  end;
  exit(false);
end;

class function Enum.All<T>(AEnum: IEnum<T>; const APredicate: TPredicate<T>): boolean;
var
  v: T;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    v := e.Current;
    if not APredicate(v) then
      exit(false);
  end;
  exit(true);
end;

class function Enum.Any<T>(AEnum: IEnum<T>; const APredicate: TPredicate<T>): boolean;
var
  v: T;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    v := e.Current;
    if APredicate(v) then
      exit(true);
  end;
  exit(false);
end;

class procedure Enum.Apply<T>(AEnum: IEnum<T>; const AFunc: TApplyProc<T>);
var
  v: T;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    v := e.Current;
    AFunc(v);
  end;
end;

class function Enum.Cache<T>(AEnum: IEnum<T>): IEnum<T>;
var
  res: IEnum<T>;
begin
  if Enum.TryGetCached<T>(AEnum, res) then
    exit(res);
  exit(TEnumCache<T>.Create(AEnum).GetEnum);
end;

class function Enum.Cache<T>(AEnum: TList<T>): IEnum<T>;
begin
  exit(TEnumCache<T>.Create(AEnum).GetEnum);
end;

class function Enum.Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T; AComparer: IComparer<T>): boolean;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    if AComparer.Compare(e.Current, AValue) = 0 then
      exit(true);
  end;
  exit(false);
end;

class function Enum.Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T; AComparer: TEqualityComparer<T>): boolean;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    if AComparer(e.Current, AValue) then
      exit(true);
  end;
  exit(false);
end;

class function Enum.Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T; AComparer: IEqualityComparer<T>): boolean;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    if AComparer.Equals(e.Current, AValue) then
      exit(true);
  end;
  exit(false);
end;

class function Enum.Contains<T>(AEnum: IEnum<T>; const [ref] AValue: T): boolean;
begin
  exit(Enum.Contains<T>(AEnum, AValue, System.Generics.Defaults.TComparer<T>.Default));
end;

class function Enum.Count<T>(AEnum: IEnum<T>): integer;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := 0;
  while e.HasMore do
    inc(result);
end;

class procedure Enum.Delete<T, TValue>(AEnum: IEnum<T>; const ATarget: TDictionary<T, TValue>);
var
  idx: integer;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  while e.HasMore do
  begin
    ATarget.Remove(e.Current);
  end;
end;

class procedure Enum.Delete<T>(AEnum: IEnum<T>; const ATarget: TList<T>; AComparator: IComparer<T>);
var
  idx: integer;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  if supports(e, ISortedEnum) then
  begin
    if AComparator = nil then
      AComparator := System.Generics.Defaults.TComparer<T>.Default;
    while e.HasMore do
    begin
      if ATarget.BinarySearch(e.Current, idx, AComparator) then
        ATarget.Delete(idx);
    end;
  end
  else
  begin
    while e.HasMore do
    begin
      ATarget.Remove(e.Current);
    end;
  end;
end;

class procedure Enum.Delete<T>(AEnum: IEnum<T>; var ATarget: TArray<T>; AComparator: IComparer<T>);
var
  idx: integer;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  if supports(e, ISortedEnum) then
  begin
    if AComparator = nil then
      AComparator := System.Generics.Defaults.TComparer<T>.Default;
    while e.HasMore do
    begin
      if TArray.BinarySearch<T>(ATarget, e.Current, idx, AComparator) then
        System.Delete(ATarget, idx, 1);
    end;
  end
  else
  begin
    while e.HasMore do
    begin
      if TArray.IndexOf<T>(ATarget, e.Current, AComparator, idx) then
        System.Delete(ATarget, idx, 1);
    end;
  end;
end;
{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}

class function Enum.IGroupBy<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): IDictionary<TKeyType, T>;
begin
  exit(Enum.IGroupBy<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      exit(A);
    end));
end;

class function Enum.IGroupBy<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, TValueType>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TCollections.CreateDictionary<TKeyType, TValueType>();
  extractor := StreamCache.GetExtractor(RttiCtx.GetType(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
    val := TValue.From<T>(valueT);
    extractor.GetValue(val, keyVal);
    result.AddOrSetValue(keyVal.AsType<TKeyType>(), AFunction(valueT));
  end;
end;

class function Enum.IGroupToLists<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): IDictionary<TKeyType, IList<T>>;
begin
  exit(Enum.IGroupToLists<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      exit(A);
    end));
end;

class function Enum.IGroupToLists<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): IDictionary<TKeyType, IList<TValueType>>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  lst: IList<TValueType>;
  key: TKeyType;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TCollections.CreateDictionary<TKeyType, IList<TValueType>>;
  extractor := StreamCache.GetExtractor(RttiCtx.GetType(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
    val := TValue.From<T>(valueT);
    if not extractor.GetValue(val, keyVal) then
      raise EStream.CreateFmt('Key ''%s'' not found', [AField.Field]);
    key := keyVal.AsType<TKeyType>();
    if not result.TryGetValue(key, lst) then
    begin
      lst := TCollections.CreateList<TValueType>();
      result.Add(key, lst);
    end;
    lst.Add(AFunction(valueT));
  end;
end;
{$ENDIF}

class function Enum.GroupBy<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TDictionary<TKeyType, TValueType>.Create();
  extractor := StreamCache.GetExtractor(RttiCtx.GetType(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
    val := TValue.From<T>(valueT);
    extractor.GetValue(val, keyVal);
    result.AddOrSetValue(keyVal.AsType<TKeyType>(), AFunction(valueT));
  end;
end;

class function Enum.GroupBy<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, T>;
begin
  exit(Enum.GroupBy<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      exit(A);
    end));
end;

class function Enum.GroupToArray<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  lst: TArray<TValueType>;
  key: TKeyType;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TDictionary < TKeyType, TArray < TValueType >>.Create();
  extractor := StreamCache.GetExtractor(RttiCtx.GetType(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
    val := TValue.From<T>(valueT);
    if not extractor.GetValue(val, keyVal) then
      raise EStream.CreateFmt('Key ''%s'' not found', [AField.Field]);
    key := keyVal.AsType<TKeyType>();
    if not result.TryGetValue(key, lst) then
    begin
      lst := [AFunction(valueT)];
      result.Add(key, lst);
    end
    else
    begin
      lst := lst + [AFunction(valueT)];
      result.AddOrSetValue(key, lst);
    end;
  end;
end;

class function Enum.GroupToArray<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TArray<T>>;
begin
  exit(Enum.GroupToArray<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      exit(A);
    end));
end;

class function Enum.GroupToLists<T, TKeyType, TValueType>(AEnum: IEnum<T>; AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>;
var
  extractor: IFieldExtractor;
  valueT: T;
  val, keyVal: TValue;
  lst: TList<TValueType>;
  key: TKeyType;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  result := TDictionary < TKeyType, TList < TValueType >>.Create();
  extractor := StreamCache.GetExtractor(RttiCtx.GetType(typeinfo(T)), AField.Field);
  while e.HasMore do
  begin
    valueT := e.Current;
    val := TValue.From<T>(valueT);
    if not extractor.GetValue(val, keyVal) then
      raise EStream.CreateFmt('Key ''%s'' not found', [AField.Field]);
    key := keyVal.AsType<TKeyType>();
    if not result.TryGetValue(key, lst) then
    begin
      lst := TList<TValueType>.Create;
      result.Add(key, lst);
    end;
    lst.Add(AFunction(valueT));
  end;
end;

class function Enum.GroupToLists<T, TKeyType>(AEnum: IEnum<T>; AField: IFieldExpr): TDictionary<TKeyType, TList<T>>;
begin
  exit(Enum.GroupToLists<T, TKeyType, T>(AEnum, AField,
    function(const A: T): T
    begin
      exit(A);
    end));
end;

class function Enum.IsCached<T>(AEnum: IEnum<T>): boolean;
var
  o: TObject;
begin
  o := AEnum as TObject;
  exit(AEnum is TCachedEnum<T>);
end;

class function Enum.Map<T, TOutput>(AEnum: IEnum<T>; const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>;
var
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  setlength(result, 0);
  while e.HasMore do
    insert(AFunction(e.Current), result, length(result));
end;

class function Enum.Max<T>(AEnum: IEnum<T>): T;
begin
  exit(Enum.Max<T>(AEnum, System.Generics.Defaults.TComparer<T>.Default));
end;

class function Enum.Max<T>(AEnum: IEnum<T>; AComparer: IComparer<T>): T;
var
  hasMax: boolean;
  v: T;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  hasMax := false;
  while e.HasMore do
  begin
    v := e.Current;
    if not hasMax or (AComparer.Compare(v, result) > 0) then
    begin
      result := v;
      hasMax := true;
    end;
  end;
  if not hasMax then
    raise EStreamItemNotFound.Create('no maximum found');
end;

class function Enum.Min<T>(AEnum: IEnum<T>): T;
begin
  result := Enum.Min<T>(AEnum, System.Generics.Defaults.TComparer<T>.Default);
end;

class function Enum.Max<T>(AEnum: IEnum<T>; const AComparer: TComparer<T>): T;
var
  hasMax: boolean;
  v: T;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  hasMax := false;
  while e.HasMore do
  begin
    v := e.Current;
    if not hasMax or (AComparer(v, result) > 0) then
    begin
      result := v;
      hasMax := true;
    end;
  end;
  if not hasMax then
    raise EStreamItemNotFound.Create('no maximum found');
end;

class function Enum.Min<T>(AEnum: IEnum<T>; const AComparer: TComparer<T>): T;
var
  hasMin: boolean;
  v: T;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  hasMin := false;
  while e.HasMore do
  begin
    v := e.Current;
    if not hasMin or (AComparer(v, result) < 0) then
    begin
      result := v;
      hasMin := true;
    end;
  end;
  if not hasMin then
    raise EStreamItemNotFound.Create('no minimim found');
end;

class function Enum.Min<T>(AEnum: IEnum<T>; AComparer: IComparer<T>): T;
var
  hasMin: boolean;
  v: T;
  e: IEnum<T>;
begin
  if not Enum.TryGetCached<T>(AEnum, e) then
    e := AEnum;
  hasMin := false;
  while e.HasMore do
  begin
    v := e.Current;
    if not hasMin or (AComparer.Compare(v, result) < 0) then
    begin
      result := v;
      hasMin := true;
    end;
  end;
  if not hasMin then
    raise EStreamItemNotFound.Create('no minimim found');
end;

class function Enum.Reverse<T>(AEnum: IEnum<T>): IEnum<T>;
var
  items: TArray<T>;
  I, j, Max: integer;
  tmp: T;
begin
  items := Enum.ToArray<T>(AEnum);
  Max := high(items);
  for I := 0 to Max div 2 do
  begin
    tmp := items[I];
    j := Max - I;
    items[I] := items[j];
    items[j] := tmp;
  end;
  exit(TArrayEnum<T>.Create(items));
end;

class function Enum.Schuffle<T>(AEnum: IEnum<T>): IEnum<T>;
var
  items: TArray<T>;
  I, j, Max: integer;
  tmp: T;

begin
  items := Enum.ToArray<T>(AEnum);
  Max := length(items);
  for I := 0 to high(items) do
  begin
    tmp := items[I];
    j := random(Max);
    items[I] := items[j];
    items[j] := tmp;
  end;
  exit(TArrayEnum<T>.Create(items));
end;

class function Enum.Sum(AEnum: IEnum<int64>): int64;
var
  e: IEnum<int64>;
begin
  if not Enum.TryGetCached<int64>(AEnum, e) then
    e := AEnum;
  result := 0;
  while e.HasMore do
    result := result + e.Current;
end;

class function Enum.Sum(AEnum: IEnum<extended>): extended;
var
  e: IEnum<extended>;
begin
  if not Enum.TryGetCached<extended>(AEnum, e) then
    e := AEnum;
  result := 0;
  while e.HasMore do
    result := result + e.Current;
end;

{$IFDEF SEMPARE_STREAMS_SPRING4D_SUPPORT}

class procedure Enum.CopyFromEnum<T>(ASource: IEnum<T>; ATarget: Spring.Collections.ICollection<T>);
begin
  while ASource.HasMore do
    ATarget.Add(ASource.Current);
end;

class function Enum.FromSpring4D<T>(ASource: Spring.Collections.IEnumerable<T>): IEnum<T>;
begin
  exit(TSpringIEnumerableEnum<T>.Create(ASource));
end;

{$ENDIF}

class function Enum.AreEqual<T>(AEnumA, AEnumB: IEnum<T>): boolean;
begin
  exit(Enum.AreEqual<T>(AEnumA, AEnumB, System.Generics.Defaults.TComparer<T>.Default));
end;

class function Enum.AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: IComparer<T>): boolean;
var
  A, B: T;
  amore, bmore: boolean;
begin
  while true do
  begin
    amore := AEnumA.HasMore;
    bmore := AEnumB.HasMore;
    if not amore and not bmore then
      exit(true);

    if amore <> bmore then
      exit(false);

    if comparator.Compare(AEnumA.Current, AEnumB.Current) <> 0 then
      exit(false);
  end;
end;

class function Enum.AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: IEqualityComparer<T>): boolean;
var
  A, B: T;
  amore, bmore: boolean;
begin
  while true do
  begin
    amore := AEnumA.HasMore;
    bmore := AEnumB.HasMore;
    if not amore and not bmore then
      exit(true);
    if amore <> bmore then
      exit(false);
    if not comparator.Equals(A, B) or not comparator.Equals(AEnumA.Current, AEnumB.Current) then
      exit(false);
  end;
  exit(false);
end;

class function Enum.AreEqual<T>(AEnumA, AEnumB: IEnum<T>; comparator: TEqualityComparer<T>): boolean;
var
  A, B: T;
  amore, bmore: boolean;
begin
  while true do
  begin
    amore := AEnumA.HasMore;
    bmore := AEnumB.HasMore;
    if not amore and not bmore then
      exit(true);

    if amore <> bmore then
      exit(false);

    if comparator(AEnumA.Current, AEnumB.Current) then
      exit(false);
  end;
end;

class function Enum.Cache<T>(AEnum: TEnumerable<T>): IEnum<T>;
begin
  exit(TEnumCache<T>.Create(AEnum).GetEnum);
end;

class function Enum.Cast<TInput, TOutput>(AEnum: IEnum<TInput>): IEnum<TOutput>;
begin
  exit(TMapEnum<TInput, TOutput>.Create(AEnum,
    function(const AInput: TInput): TOutput
    begin
      exit(AInput as TOutput);
    end));
end;

class function Enum.Average(AEnum: IEnum<int64>): extended;
var
  e: IEnum<int64>;
  c: integer;
begin
  if not Enum.TryGetCached<int64>(AEnum, e) then
    e := AEnum;
  result := 0;
  c := 0;
  while e.HasMore do
  begin
    result := result + e.Current;
    inc(c);
  end;
  if c <> 0 then
    exit(result / c);
end;

class function Enum.Average(AEnum: IEnum<extended>): extended;
var
  e: IEnum<extended>;
  c: integer;
begin
  if not Enum.TryGetCached<extended>(AEnum, e) then
    e := AEnum;
  result := 0;
  c := 0;
  while e.HasMore do
  begin
    result := result + e.Current;
    inc(c);
  end;
  if c <> 0 then
    exit(result / c);
end;
{ TCachedEnum<T> }

constructor TCachedEnum<T>.Create(Cache: IEnumCache<T>);
begin
  inherited Create();
  FCache := Cache;
  FEnum := TTEnumerableEnum<T>.Create(GetCache);
end;

function TCachedEnum<T>.Current: T;
begin
  exit(FEnum.Current);
end;

destructor TCachedEnum<T>.Destroy;
begin
  FCache := nil;
  FEnum := nil;
  inherited;
end;

function TCachedEnum<T>.EOF: boolean;
begin
  exit(FEnum.EOF);
end;

function TCachedEnum<T>.GetCache: TList<T>;
begin
  exit(FCache.GetCache);
end;

function TCachedEnum<T>.GetEnum: IEnum<T>;
begin
  exit(TCachedEnum<T>.Create(FCache));
end;

procedure TCachedEnum<T>.Next;
begin
  FEnum.Next;
end;

{ TEnumerableEnum2<T> }

constructor TTEnumerableEnum<T>.Create(const AEnum: TEnumerable<T>; ASortedStatus: TSortedStatus);
begin
  inherited Create();
  FEnum := AEnum.GetEnumerator;
  FSortedStatus := ASortedStatus;
  Next;
end;

function TTEnumerableEnum<T>.Current: T;
begin
  exit(FEnum.Current);
end;

destructor TTEnumerableEnum<T>.Destroy;
begin
  FEnum.Free;
  inherited;
end;

function TTEnumerableEnum<T>.EOF: boolean;
begin
  exit(FEof);
end;

function TTEnumerableEnum<T>.GetSortedStatus: TSortedStatus;
begin
  exit(FSortedStatus);
end;

procedure TTEnumerableEnum<T>.Next;
begin
  FEof := not FEnum.MoveNext;
end;

{ TSortedEnum<T> }

constructor TSortedEnum<T>.Create(Enum: IEnum<T>; comparator: IComparer<T>);
var
  v: T;
  I: integer;
begin
  FItems := TList<T>.Create;
  while Enum.HasMore do
  begin
    v := Enum.Current;
    FItems.BinarySearch(v, I, comparator);
    FItems.insert(I, v);
  end;
  inherited Create(TTEnumerableEnum<T>.Create(FItems, ssSorted));
end;

destructor TSortedEnum<T>.Destroy;
begin
  FItems.Free;
  inherited;
end;

function TSortedEnum<T>.GetCache: TList<T>;
begin
  exit(FItems);
end;

function TSortedEnum<T>.GetEnum: IEnum<T>;
begin
  exit(TTEnumerableEnum<T>.Create(FItems, ssSorted));
end;

{ TJoinEnum<TLeft, TRight, TJoined> }

constructor TJoinEnum<TLeft, TRight, TJoined>.Create( //
  AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; //
const AOn: TJoinOnFunction<TLeft, TRight>; const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
begin
  inherited Create();
  FEnumLeft := AEnumLeft;
  FEnumRight := Enum.Cache<TRight>(AEnumRight);
  FOn := AOn;
  FSelect := ASelect;
  FHasLeft := FEnumLeft.HasMore;
  FHasRight := FEnumRight.HasMore;
  FindNext;
end;

function TJoinEnum<TLeft, TRight, TJoined>.Current: TJoined;
begin
  exit(FNext);
end;

destructor TJoinEnum<TLeft, TRight, TJoined>.Destroy;
begin
  FEnumLeft := nil;
  FEnumRight := nil;
  inherited;
end;

function TJoinEnum<TLeft, TRight, TJoined>.EOF: boolean;
begin
  exit(not FHasNext);
end;

procedure TJoinEnum<TLeft, TRight, TJoined>.FindNext;
var
  Left: TLeft;
  Right: TRight;
  match: boolean;
begin
  FHasNext := false;
  while FHasLeft and FHasRight do
  begin
    Left := FEnumLeft.Current;
    Right := FEnumRight.Current;

    match := FOn(Left, Right);
    if match then
    begin
      FHasNext := true;
      FNext := FSelect(Left, Right);
    end;

    FHasRight := FEnumRight.HasMore;
    if not FHasRight then
    begin
      FHasLeft := FEnumLeft.HasMore;
      ResetRight;
    end;
    if match then
      exit;
  end;
end;

procedure TJoinEnum<TLeft, TRight, TJoined>.Next;
begin
  FindNext;
end;

procedure TJoinEnum<TLeft, TRight, TJoined>.ResetRight;
var
  res: IEnum<TRight>;
begin
  if Enum.TryGetCached<TRight>(FEnumRight, res) then
    FEnumRight := res;
  FHasRight := FEnumRight.HasMore;
end;

{ TUnionEnum<T> }

constructor TUnionEnum<T>.Create(Enum: TArray < IEnum < T >> );
begin
  inherited Create();
  FEnums := Enum;
  FIdx := 0;
end;

function TUnionEnum<T>.Current: T;
begin
  exit(FEnums[FIdx].Current);
end;

destructor TUnionEnum<T>.Destroy;
begin
  FEnums := nil;
  inherited;
end;

function TUnionEnum<T>.EOF: boolean;
begin
  if (length(FEnums) = 0) or (FIdx >= length(FEnums)) then
    exit(true);
  exit(FEnums[FIdx].EOF);
end;

procedure TUnionEnum<T>.Next;
begin
  FEnums[FIdx].Next;
  if EOF then
    inc(FIdx);
end;

{ TLeftJoinEnum<TLeft, TRight, TJoined> }

constructor TLeftJoinEnum<TLeft, TRight, TJoined>.Create(AEnumLeft: IEnum<TLeft>; AEnumRight: IEnum<TRight>; const AOn: TJoinOnFunction<TLeft, TRight>;
const ASelect: TJoinSelectFunction<TLeft, TRight, TJoined>);
begin
  inherited Create();
  FEnumLeft := AEnumLeft;
  // if not cached, we cache it
  // if it is cached, we get a new enum
  if not Enum.TryGetCached<TRight>(AEnumRight, FEnumRight) then
    FEnumRight := Enum.Cache<TRight>(AEnumRight);
  FOn := AOn;
  FSelect := ASelect;
  FHasLeft := FEnumLeft.HasMore;
  FHasRight := FEnumRight.HasMore;
  FFoundRight := false;
  FindNext;
end;

function TLeftJoinEnum<TLeft, TRight, TJoined>.Current: TJoined;
begin
  exit(FNext);
end;

destructor TLeftJoinEnum<TLeft, TRight, TJoined>.Destroy;
begin
  FEnumLeft := nil;
  FEnumRight := nil;
  inherited;
end;

function TLeftJoinEnum<TLeft, TRight, TJoined>.EOF: boolean;
begin
  exit(not FHasNext);
end;

procedure TLeftJoinEnum<TLeft, TRight, TJoined>.FindNext;
var
  Left: TLeft;
  Right: TRight;
  match: boolean;
begin
  FHasNext := false;
  while FHasLeft and FHasRight do
  begin
    Left := FEnumLeft.Current;
    Right := FEnumRight.Current;
    match := FOn(Left, Right);
    if match then
    begin
      FHasNext := true;
      FFoundRight := true;
      FNext := FSelect(Left, Right);
    end;
    FHasRight := FEnumRight.HasMore;
    if not FHasRight then
    begin
      if not FFoundRight then
      begin
        fillchar(Right, sizeof(Right), 0);
        FHasNext := true;
        FNext := FSelect(Left, Right);
        match := true;
      end;

      FHasLeft := FEnumLeft.HasMore;
      ResetRight;
    end;
    if match then
      exit;
  end;
end;

procedure TLeftJoinEnum<TLeft, TRight, TJoined>.Next;
begin
  FindNext;
end;

procedure TLeftJoinEnum<TLeft, TRight, TJoined>.ResetRight;
var
  res: IEnum<TRight>;
begin
  if Enum.TryGetCached<TRight>(FEnumRight, res) then
    FEnumRight := res;
  FFoundRight := false;
  FHasRight := FEnumRight.HasMore;
end;

{ TUniqueEnum<T> }

constructor TUniqueEnum<T>.Create(Enum: IEnum<T>; comparator: IComparer<T>);
var
  v: T;
  I: integer;
begin
  FItems := TList<T>.Create;
  while Enum.HasMore do
  begin
    v := Enum.Current;
    if not FItems.BinarySearch(v, I, comparator) then
      FItems.insert(I, v);
  end;
  inherited Create(TTEnumerableEnum<T>.Create(FItems, ssSorted));
end;

destructor TUniqueEnum<T>.Destroy;
begin
  FItems.Free;
  inherited;
end;

function TUniqueEnum<T>.GetCache: TList<T>;
begin
  exit(FItems);
end;

function TUniqueEnum<T>.GetEnum: IEnum<T>;
begin
  exit(TTEnumerableEnum<T>.Create(FItems, ssSorted));
end;

{ TDataSetEnumClass<T> }

constructor TDataSetEnumClass<T>.Create(const ADataSet: TDataSet);
var
  attrib: TCustomAttribute;
  Field: TRttiField;
  fieldname: string;
begin
  FDataSet := ADataSet;
  FDataSet.First;
  FFields := TDictionary<string, string>.Create;

  FRttiType := RttiCtx.GetType(typeinfo(T));
  FConstructor := FRttiType.GetMethod('Create');
  for Field in FRttiType.GetFields do
  begin
    fieldname := Field.name;
    for attrib in Field.GetAttributes do
    begin
      if attrib is StreamFieldAttribute then
      begin
        fieldname := StreamFieldAttribute(attrib).name;
        break;
      end;
    end;
    FFields.AddOrSetValue(Field.name, fieldname);
  end;
end;

function TDataSetEnumClass<T>.Current: T;
var
  Field: TRttiField;
  obj: TObject;
begin
  obj := FConstructor.Invoke(FRttiType.AsInstance.MetaclassType, []).AsObject;
  move(obj, result, sizeof(T)); // this is a hack
  for Field in FRttiType.GetFields do
  begin
    Field.SetValue(obj, TValue.fromVariant(FDataSet.FieldByName(FFields[Field.name]).AsVariant));
  end;
end;

destructor TDataSetEnumClass<T>.Destroy;
begin
  FFields.Free;
  inherited;
end;

function TDataSetEnumClass<T>.EOF: boolean;
begin
  exit(FDataSet.EOF);
end;

procedure TDataSetEnumClass<T>.Next;
begin
  FDataSet.Next;
end;

{ TDataSetEnumRecord<T> }

constructor TDataSetEnumRecord<T>.Create(const ADataSet: TDataSet);
var
  attrib: TCustomAttribute;
  Field: TRttiField;
  fieldname: string;
begin
  FDataSet := ADataSet;
  FDataSet.First;
  FFields := TDictionary<string, string>.Create;

  FRttiType := RttiCtx.GetType(typeinfo(T));
  for Field in FRttiType.GetFields do
  begin
    fieldname := Field.name;
    for attrib in Field.GetAttributes do
    begin
      if attrib is StreamFieldAttribute then
      begin
        fieldname := StreamFieldAttribute(attrib).name;
        break;
      end;
    end;
    FFields.AddOrSetValue(Field.name, fieldname);
  end;

end;

function TDataSetEnumRecord<T>.Current: T;
var
  Field: TRttiField;

begin
  fillchar(result, sizeof(T), 0);
  for Field in FRttiType.GetFields do
  begin
    Field.SetValue(@result, TValue.fromVariant(FDataSet.FieldByName(FFields[Field.name]).AsVariant));
  end;
end;

destructor TDataSetEnumRecord<T>.Destroy;
begin
  FFields.Free;
  inherited;
end;

function TDataSetEnumRecord<T>.EOF: boolean;
begin
  exit(FDataSet.EOF);
end;

procedure TDataSetEnumRecord<T>.Next;
begin
  FDataSet.Next;
end;

{ TIntRange }

constructor TIntRangeEnum.Create(const AStart, AEnd: int64; const ADelta: int64);
begin
  inherited Create();
  FIdx := AStart;
  FEnd := AEnd;
  FDelta := ADelta;
end;

function TIntRangeEnum.Current: int64;
begin
  exit(FIdx);
end;

function TIntRangeEnum.EOF: boolean;
begin
  exit(FIdx > FEnd);
end;

procedure TIntRangeEnum.Next;
begin
  inc(FIdx);
end;

{ TFloatRange }

constructor TFloatRangeEnum.Create(const AStart, AEnd, ADelta: extended);
begin
  FIdx := AStart;
  FEnd := AEnd;
  FDelta := ADelta;
end;

function TFloatRangeEnum.Current: extended;
begin
  exit(FIdx);
end;

function TFloatRangeEnum.EOF: boolean;
begin
  exit(FIdx > FEnd);
end;

procedure TFloatRangeEnum.Next;
begin
  FIdx := FIdx + FDelta;
end;

{ TStringEnum }

constructor TStringEnum.Create(const AValue: string);
begin
  FValue := AValue;
  FIdx := 1;
  FEnd := length(AValue);
end;

function TStringEnum.Current: char;
begin
  exit(FValue[FIdx]);
end;

function TStringEnum.EOF: boolean;
begin
  exit(FIdx > FEnd);
end;

procedure TStringEnum.Next;
begin
  inc(FIdx);
end;

{ TArrayHelper }

class function TArrayHelper.IndexOf<T>(const ATarget: TArray<T>; const [ref] AValue: T; AComparator: IComparer<T>; out idx: integer): boolean;
var
  I: integer;
begin
  for I := 0 to high(ATarget) do
  begin
    if AComparator.Compare(AValue, ATarget[I]) = 0 then
    begin
      idx := I;
      exit(true);
    end;
  end;
  exit(false);
end;

initialization

SortString := TStringComparer.Create();
SortInt64 := TIntegerComparer.Create();
SortDouble := TDoubleComparer.Create();
SortBoolean := TBooleanComparer.Create();

StreamCache := TStreamTypeCache.Create;

finalization

SortString := nil;
SortInt64 := nil;
SortDouble := nil;
SortBoolean := nil;

StreamCache.Free;

end.
