unit Sempare.Streams;

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
    FExpr: TExpr;
  public
    constructor Create(const AExpr: TExpr);
    class operator LogicalAnd(const ALeft: boolean; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalAnd(const ALeft: TExpression; const ARight: boolean): TExpression; overload; static;
    class operator LogicalAnd(const ALeft: TExpression; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalOr(const ALeft: boolean; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalOr(const ALeft: TExpression; const ARight: TExpression): TExpression; overload; static;
    class operator LogicalOr(const ALeft: TExpression; const ARight: boolean): TExpression; overload; static;
    class operator LogicalNot(const AExpr: TExpression): TExpression; overload; static;
    property Expr: TExpr read FExpr;
  end;

  /// <summary>
  /// TFieldExpression is a record helper used to represent binary expressions (=, &lt;&gt; &lt;=, &lt;, &gt;, &gt;=). <para/>
  /// <code>
  /// var fld : TFieldExpression := field('name') = 'sarah';
  /// </code>
  /// </summary>
  TFieldExpression = record
  strict private
    FExpr: TFieldExpr;
  public
    constructor Create(const AExpr: TFieldExpr);
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

    property FieldExpr: TFieldExpr read FExpr;
  end;

  /// <summary>
  /// TSortExpression is a record helper used to represent sort constaints. <para/>
  /// <code>
  /// var fld : TSortExpression := field('name', ASC) and field('age', DESC) ;
  /// </code>
  /// </summary>
  TSortExpression = record
  strict private
    FExprs: TArray<TSortExpr>;
  public
    constructor Create(const AExpr: TSortExpr); overload;
    constructor Create(const AExprs: TArray<TSortExpr>); overload;

    class operator Implicit(const AField: TFieldExpression): TSortExpression; static;
    class function Field(const AName: string; const AOrder: TSortOrder = soAscending): TSortExpression; static;
    class operator LogicalAnd(const ALeft: TSortExpression; const ARight: TSortExpression): TSortExpression; overload; static;

    property Exprs: TArray<TSortExpr> read FExprs;
  end;

  TFilteredStream<T> = record
  strict private
    FEnum: IEnum<T>;
  public
    constructor Create(Enum: IEnum<T>);

    function SortBy(const AExpr: TSortExpression): TFilteredStream<T>;
    function TakeOne: T;
    function ToArray(): TArray<T>;
    function ToList(): TList<T>;
    function Take(const ANumber: integer): TFilteredStream<T>; overload;
    function Skip(const ANumber: integer): TFilteredStream<T>; overload;
    function Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TFilteredStream<TOutput>;
    procedure Apply(const AFunction: TApplyFunction<T>);
    // function GroupBy<TFieldType>(const AField: string): TDictionary<TFieldType, TList<T>>; overload;
    // function GroupBy<TFieldType>(const AField: string): TDictionary<TFieldType, T>; overload;
    // function GroupBy<TFieldType>(const AFunc: TMapFunction<TFieldType, T>): TDictionary<TFieldType, TData>; overload;
    function Count: integer;

    //property Enum: IEnum<T> read FEnum;
  end;

  TStreamOperation<T> = record
  strict private
    FEnum: IEnum<T>;
  public
    constructor Create(Enum: IEnum<T>);

    function SortBy(const AExpr: TSortExpression): TFilteredStream<T>;

    function Filter(const ACondition: TExpression): TFilteredStream<T>; overload;
    function Filter(const ACondition: TFilterFunction<T>): TFilteredStream<T>; overload;
    function Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>;
    procedure Apply(const AFunction: TApplyFunction<T>);
    // function GroupBy<TFieldType>(const AField: string): TDictionary<TFieldType, T>; overload;
    // function GroupBy<TFieldType, TData>(const AField: string): TDictionary<TFieldType, TData>; overload;
    function Count: integer;
    //property Enum: IEnum<T> read FEnum;
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
  /// FirstName : TFieldExpression;<para/>
  /// [StreamRef('FLN')]<para/>
  /// LastName : TFieldExpression;<para/>
  /// [StreamRef('FAge')]<para/>
  /// Age : TFieldExpression;<para/>
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
    /// Stream from a dynarray (TList&lt;T&gt;) source.
    /// </summary>
    /// <param name="ASource">A source of type TList.</param>
    /// <returns>TStreamOperation&lt;T&gt; allowing additional operations on the TList source.</returns>;
    class function From<T>(const ASource: TList<T>): TStreamOperation<T>; overload; static;

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

constructor TSortExpression.Create(const AExprs: TArray<TSortExpr>);
begin
  FExprs := AExprs;
end;

constructor TSortExpression.Create(const AExpr: TSortExpr);
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
  e: TSortExpr;
begin
  result.FExprs := ALeft.FExprs;
  for e in ARight.FExprs do
    insert(e, result.FExprs, length(result.FExprs));
end;

{ TExpression }

constructor TExpression.Create(const AExpr: TExpr);
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

function GetExpr(const AExpr: TFieldExpr; const AOP: TFieldExpr.TOper; const AValue: TValue): TFieldExpr; inline;
begin
  AExpr.OP := AOP;
  AExpr.Value := AValue;
  result := AExpr;
end;

constructor TFieldExpression.Create(const AExpr: TFieldExpr);
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
  result := TExpression.Create(GetExpr(AField.FExpr, foEQ, TValue.From<TFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.GreaterThan(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGT, TValue.From<TFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.GreaterThanOrEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foGTE, TValue.From<TFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.LessThan(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLT, TValue.From<TFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.LessThanOrEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foLTE, TValue.From<TFieldExpr>(AValue.FExpr)));
end;

class operator TFieldExpression.NotEqual(const AField: TFieldExpression; const AValue: TFieldExpression): TExpression;
begin
  result := TExpression.Create(GetExpr(AField.FExpr, foNEQ, TValue.From<TFieldExpr>(AValue.FExpr)));
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

{ TFilteredStream<T> }

function TFilteredStream<T>.Count: integer;
begin
  result := Enum.Count<T>(FEnum);
end;

constructor TFilteredStream<T>.Create(Enum: IEnum<T>);
begin
  FEnum := Enum;
end;

function TFilteredStream<T>.Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TFilteredStream<TOutput>;
begin
  result := TFilteredStream<TOutput>.Create(TMapEnum<T, TOutput>.Create(FEnum, AFunction));
end;

function TFilteredStream<T>.SortBy(const AExpr: TSortExpression): TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create( //
    TSortedEnum<T>.Create(FEnum, //
    TSortFieldComposite<T>.Create(AExpr.Exprs)) //
    );
end;

function TFilteredStream<T>.Skip(const ANumber: integer): TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create(TSkip<T>.Create(FEnum, ANumber));
end;

function TFilteredStream<T>.Take(const ANumber: integer): TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create(ttake<T>.Create(FEnum, ANumber));
end;

function TFilteredStream<T>.TakeOne: T;
var
  res: TArray<T>;
begin
  res := Enum.ToArray<T>(ttake<T>.Create(FEnum, 1));
  if length(res) <> 1 then
    raise EStream.Create('Expecting one item');
  result := res[0];
  res := nil;
end;

function TFilteredStream<T>.ToArray: TArray<T>;
begin
  result := Enum.ToArray<T>(FEnum);
end;

function TFilteredStream<T>.ToList: TList<T>;
begin
  result := Enum.ToList<T>(FEnum);
end;

procedure TFilteredStream<T>.Apply(const AFunction: TApplyFunction<T>);
begin
  Enum.Apply<T>(FEnum, AFunction);
end;

{ TStreamOperation<T> }

function TStreamOperation<T>.Count: integer;
begin
  result := Enum.Count<T>(FEnum);
end;

function TStreamOperation<T>.Filter(const ACondition: TExpression): TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create( //
    TFilterEnum<T>.Create(FEnum, //
    TExprFilter<T>.Create(ACondition.Expr) //
    ) //
    );
end;

constructor TStreamOperation<T>.Create(Enum: IEnum<T>);
begin
  FEnum := Enum;
end;

function TStreamOperation<T>.Filter(const ACondition: TFilterFunction<T>): TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create( //
    TFilterEnum<T>.Create(FEnum, //
    TTypedFunctionFilter<T>.Create(ACondition) //
    ) //
    );
end;

function TStreamOperation<T>.SortBy(const AExpr: TSortExpression): TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create( //
    TSortedEnum<T>.Create(FEnum, //
    TSortFieldComposite<T>.Create(AExpr.Exprs)) //
    );
end;

function TStreamOperation<T>.Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>;
begin
  result := Enum.Map<T, TOutput>(FEnum, AFunction);
end;

procedure TStreamOperation<T>.Apply(const AFunction: TApplyFunction<T>);
begin
  Enum.Apply<T>(FEnum, AFunction);
end;

{ Stream }

class function Stream.From<T>(const ASource: TArray<T>): TStreamOperation<T>;
begin
  result := TStreamOperation<T>.Create(TArrayEnum<T>.Create(ASource));
end;

class function Stream.From<T>(const ASource: TEnumerable<T>): TStreamOperation<T>;
begin
  result := TStreamOperation<T>.Create(TEnumerableEnum2<T>.Create(ASource));
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
  FillChar(result, sizeof(result), 0);
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
  result := TStreamOperation<T>.Create(TEnumerableEnum<T>.Create(ASource));
end;

class function Stream.From<T>(const ASource: TList<T>): TStreamOperation<T>;
begin
  result := TStreamOperation<T>.Create(TEnumerableEnum2<T>.Create(ASource));
end;

{ StreamFieldAttribute }

constructor StreamFieldAttribute.Create(const AName: string);
begin
  FName := AName;
end;

end.
