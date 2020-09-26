unit Sempare.Streams;
{$optimization off}

interface

uses
  System.Generics.Collections,
  System.Generics.Defaults,
  System.TypInfo,
  System.Rtti,
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

    function GetEnum: IEnum<T>;
    function IsCached: boolean;

  public
    class operator Implicit(Enum: IEnum<T>): TStreamOperation<T>; static;


    // class operator Implicit(Enum: IEnum<T>): TStreamOperation<T>; static;

    function SortBy(const AExpr: TSortExpression): TStreamOperation<T>;
    function TakeOne: T;
    function ToArray(): TArray<T>;
    function ToList(): TList<T>;
    function Take(const ANumber: integer): TStreamOperation<T>; overload;
    function Skip(const ANumber: integer): TStreamOperation<T>; overload;
    function Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TStreamOperation<TOutput>;
    procedure Apply(const AFunction: TApplyFunction<T>);

    function Cache: TStreamOperation<T>;

    function InnerJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;
    function LeftJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;
    function RightJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;
    function FullJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
      : TStreamOperation<TJoined>;

    function Filter(const ACondition: TExpression): TStreamOperation<T>; overload;
    function Filter(const ACondition: TFilterFunction<T>): TStreamOperation<T>; overload;

    function GroupBy<TKeyType>(AField: IFieldExpr): TDictionary<TKeyType, T>; overload;
    function GroupBy<TKeyType, TValueType>(AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>; overload;
    function GroupToLists<TKeyType>(AField: IFieldExpr): TDictionary<TKeyType, TList<T>>; overload;
    function GroupToLists<TKeyType, TValueType>(AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>; overload;
    function GroupToArray<TKeyType>(AField: IFieldExpr): TDictionary<TKeyType, TArray<T>>; overload;
    function GroupToArray<TKeyType, TValueType>(AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>; overload;

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
  /// StreamRef attribute allows for referencing between a class definition and the metadata record.
  /// </summary>
  StreamFieldAttribute = class(TCustomAttribute)
  private
    FName: string;
  public

    /// <summary>
    /// StreamRef attribute allows for referencing between a class definition and the metadata record.
    /// </summary>
    /// <param name="AName">The name of field in the referenced class</param>
    constructor Create(const AName: string);

    /// <summary>
    /// The name of the field being referenced.
    /// </summary>
    property Name: string read FName;
  end;

function Field(const AName: string): TFieldExpression; overload;
function Field(const AName: string; const AOrder: TSortOrder): TSortExpression; overload;
function Self(): TFieldExpression;

implementation

uses
  Sempare.Streams.Rtti;

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
    result := Enum.Cache<T>(GetEnum);
end;

function TStreamOperation<T>.Count: integer;
begin
  result := Enum.Count<T>(GetEnum);
end;

class operator TStreamOperation<T>.Implicit(Enum: IEnum<T>): TStreamOperation<T>;
begin
  result.FEnum := Enum;
end;

function TStreamOperation<T>.GetEnum: IEnum<T>;
begin
  result := FEnum;
end;

function TStreamOperation<T>.GroupBy<TKeyType, TValueType>(AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TValueType>;
begin
  result := Enum.GroupBy<T, TKeyType, TValueType>(GetEnum, AField, AFunction);
end;

function TStreamOperation<T>.GroupBy<TKeyType>(AField: IFieldExpr): TDictionary<TKeyType, T>;
begin
  result := Enum.GroupBy<T, TKeyType>(GetEnum, AField);
end;

function TStreamOperation<T>.GroupToArray<TKeyType, TValueType>(AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TArray<TValueType>>;
begin
  result := Enum.GroupToArray<T, TKeyType, TValueType>(GetEnum, AField, AFunction);
end;

function TStreamOperation<T>.GroupToArray<TKeyType>(AField: IFieldExpr): TDictionary<TKeyType, TArray<T>>;
begin
  result := Enum.GroupToArray<T, TKeyType>(GetEnum, AField);
end;

function TStreamOperation<T>.GroupToLists<TKeyType, TValueType>(AField: IFieldExpr; const AFunction: TMapFunction<T, TValueType>): TDictionary<TKeyType, TList<TValueType>>;
begin
  result := Enum.GroupToLists<T, TKeyType, TValueType>(GetEnum, AField, AFunction);
end;

function TStreamOperation<T>.GroupToLists<TKeyType>(AField: IFieldExpr): TDictionary<TKeyType, TList<T>>;
begin
  result := Enum.GroupToLists<T, TKeyType>(GetEnum, AField);
end;

function TStreamOperation<T>.IsCached: boolean;
begin
  result := supports(FEnum, IEnumCache<T>);
end;

function TStreamOperation<T>.InnerJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  result := TjoinEnum<T, TOther, TJoined>.Create( //
    GetEnum, AOther.GetEnum, AOn, ASelect);
end;

function TStreamOperation<T>.LeftJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
begin
  result := TLeftJoinEnum<T, TOther, TJoined>.Create(GetEnum, AOther.GetEnum, AOn, ASelect);
end;

function TStreamOperation<T>.Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TStreamOperation<TOutput>;
begin
  result := TMapEnum<T, TOutput>.Create(GetEnum, AFunction);
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

function TStreamOperation<T>.SortBy(const AExpr: TSortExpression): TStreamOperation<T>;
begin
  result := TSortedEnum<T>.Create(GetEnum, //
    TSortFieldComposite<T>.Create(AExpr.Exprs)); //
end;

function TStreamOperation<T>.Skip(const ANumber: integer): TStreamOperation<T>;
begin
  result := TSkip<T>.Create(GetEnum, ANumber);
end;

function TStreamOperation<T>.Take(const ANumber: integer): TStreamOperation<T>;
begin
  result := ttake<T>.Create(GetEnum, ANumber);
end;

function TStreamOperation<T>.TakeOne: T;
var
  res: TArray<T>;
begin
  res := Enum.ToArray<T>(ttake<T>.Create(GetEnum, 1));
  if length(res) <> 1 then
    raise EStream.Create('Expecting one item');
  result := res[0];
  res := nil;
end;

function TStreamOperation<T>.ToArray: TArray<T>;
begin
  result := Enum.ToArray<T>(GetEnum);
end;

function TStreamOperation<T>.ToList: TList<T>;
begin
  result := Enum.ToList<T>(GetEnum);
end;

procedure TStreamOperation<T>.Apply(const AFunction: TApplyFunction<T>);
begin
  Enum.Apply<T>(GetEnum, AFunction);
end;

function TStreamOperation<T>.Filter(const ACondition: TExpression): TStreamOperation<T>;
begin
  result := TFilterEnum<T>.Create(GetEnum, //
    TExprFilter<T>.Create(ACondition.Expr) //
    );
end;

function TStreamOperation<T>.Filter(const ACondition: TFilterFunction<T>): TStreamOperation<T>;
begin
  result := TFilterEnum<T>.Create(GetEnum, //
    TTypedFunctionFilter<T>.Create(ACondition) //
    );
end;

function TStreamOperation<T>.FullJoin<TOther, TJoined>(const [ref] AOther: TStreamOperation<TOther>; const AOn: TJoinOnFunction<T, TOther>; const ASelect: TJoinSelectFunction<T, TOther, TJoined>)
  : TStreamOperation<TJoined>;
var
  c: TStreamOperation<TOther>;
begin
  c := Enum.Cache<TOther>(AOther.FEnum);
  result := TUnionEnum<TJoined>.Create( //
    [LeftJoin<TOther, TJoined>(c, AOn, ASelect).FEnum, //
    RightJoin<TOther, TJoined>(c, AOn, ASelect).FEnum //
    ]);
end;

{ Stream }

class function Stream.From<T>(const ASource: TArray<T>): TStreamOperation<T>;
begin
  result := TArrayEnum<T>.Create(ASource);
end;

class function Stream.From<T>(const ASource: TEnumerable<T>): TStreamOperation<T>;
begin
  result := TEnumerableEnum2<T>.Create(ASource);
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
  fieldExprType := rttiCtx.GetType(typeinfo(TFieldExpression));
  metaType := rttiCtx.GetType(typeinfo(TMetadata));
  resVal := TValue.From<TMetadata>(result);

  // resolve otherType by searching for StreamClass on metaType
  otherType := rttiCtx.GetType(typeinfo(T));;

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
      raise EStreamReflect.CreateFmt('referenced field ''%s'' not found', [name]);
    Expr := Field(name);
    exprVal := TValue.From<TFieldExpression>(Expr);
    fld.SetValue(@result, exprVal);
  end;
end;

class function Stream.From<T>(const ASource: IEnumerable<T>): TStreamOperation<T>;
begin
  result := TEnumerableEnum<T>.Create(ASource);
end;

{ StreamFieldAttribute }

constructor StreamFieldAttribute.Create(const AName: string);
begin
  FName := AName;
end;

end.
