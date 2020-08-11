unit Sempare.Streams;

interface

uses
  System.Generics.Collections,
  System.Generics.Defaults,
  System.TypInfo,
  System.Rtti,
  Sempare.Streams.Types,
  Sempare.Streams.Expr,
  Sempare.Streams.Filter,
  Sempare.Streams.Processor,
  Sempare.Streams.Sort;

const
  soAscending = Sempare.Streams.Types.TSortOrder.soAscending;
  soDescending = Sempare.Streams.Types.TSortOrder.soDescending;
  ASC = Sempare.Streams.Types.TSortOrder.ASC;
  DESC = Sempare.Streams.Types.TSortOrder.DESC;

type
  TSortOrder = Sempare.Streams.Types.TSortOrder;

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
    FProcessor: IStreamProcessor<T>;
    function GetStream(): TFilteredStream<T>;
  public
    constructor Create(const Processor: IStreamProcessor<T>);

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

    property Processor: IStreamProcessor<T> read FProcessor;
  end;

  TStreamOperation<T> = record
  strict private
    FProcessor: IStreamProcessor<T>;
    function GetStream(): TFilteredStream<T>;
  public
    constructor Create(const Processor: IStreamProcessor<T>);

    function SortBy(const AExpr: TSortExpression): TFilteredStream<T>;

    function Filter(const ACondition: TExpression): TFilteredStream<T>; overload;
    function Filter(const ACondition: TFilterFunction<T>): TFilteredStream<T>; overload;
    function Transform<TOutput>(const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>;
    procedure Apply(const AFunction: TApplyFunction<T>);
    // function GroupBy<TFieldType>(const AField: string): TDictionary<TFieldType, T>; overload;
    // function GroupBy<TFieldType, TData>(const AField: string): TDictionary<TFieldType, TData>; overload;
    function Count: integer;
    property Processor: IStreamProcessor<T> read FProcessor;
  end;

  Stream = record
  public
    class function From<T>(const AType: IEnumerable<T>): TStreamOperation<T>; overload; static;
    class function From<T>(const AType: TArray<T>): TStreamOperation<T>; overload; static;
    class function From<T>(const AType: TList<T>): TStreamOperation<T>; overload; static;
    class function Filter<T>(const Filter: TFilterFunction<T>): TExpression; static;
  end;

function Field(const AName: string): TFieldExpression; overload;
function Field(const AName: string; const AOrder: TSortOrder): TSortExpression; overload;

implementation

uses
  System.SysUtils,
  Sempare.Streams.RttiCache;

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
  result := FProcessor.Count;
end;

constructor TFilteredStream<T>.Create(const Processor: IStreamProcessor<T>);
begin
  FProcessor := Processor;
end;

function TFilteredStream<T>.GetStream: TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create(FProcessor.Clone);
end;

function TFilteredStream<T>.Map<TOutput>(const AFunction: TMapFunction<T, TOutput>): TFilteredStream<TOutput>;
var
  A: TArray<TOutput>;
begin
  A := Processor.AsStreamProcessor.Map<T, TOutput>(AFunction);
  result := TFilteredStream<TOutput>.Create(TArrayStreamProcessor<TOutput>.Create(TValue.From < TArray < TOutput >> (A)));
end;

function TFilteredStream<T>.SortBy(const AExpr: TSortExpression): TFilteredStream<T>;
begin
  result := GetStream;
  result.Processor.SortExpr := AExpr.Exprs;
end;

function TFilteredStream<T>.Skip(const ANumber: integer): TFilteredStream<T>;
begin
  result := GetStream;
  result.Processor.Skip(ANumber);
end;

function TFilteredStream<T>.Take(const ANumber: integer): TFilteredStream<T>;
begin
  result := GetStream;
  result.Processor.Take(ANumber);
end;

function TFilteredStream<T>.TakeOne: T;
begin
  result := FProcessor.TakeOne;
end;

function TFilteredStream<T>.ToArray: TArray<T>;
begin
  result := FProcessor.ToArray();
end;

function TFilteredStream<T>.ToList: TList<T>;
begin
  result := FProcessor.ToList();
end;

procedure TFilteredStream<T>.Apply(const AFunction: TApplyFunction<T>);
begin
  FProcessor.Apply(AFunction);
end;

{ TStreamOperation<T> }

function TStreamOperation<T>.Count: integer;
begin
  result := FProcessor.Count;
end;

function TStreamOperation<T>.Filter(const ACondition: TExpression): TFilteredStream<T>;
begin
  result := GetStream;
  result.Processor.Filter := TDynamicFilterProcessor.Create(ACondition.Expr, FProcessor.RttiType);
end;

constructor TStreamOperation<T>.Create(const Processor: IStreamProcessor<T>);
begin
  FProcessor := Processor;
end;

function TStreamOperation<T>.Filter(const ACondition: TFilterFunction<T>): TFilteredStream<T>;
begin
  result := GetStream;
  result.Processor.Filter := TTypedFunctionFilterProcessor<T>.Create(ACondition);
end;

function TStreamOperation<T>.GetStream: TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create(FProcessor.Clone);
end;

function TStreamOperation<T>.SortBy(const AExpr: TSortExpression): TFilteredStream<T>;
begin
  result := TFilteredStream<T>.Create(Processor);
  Processor.SortExpr := AExpr.Exprs;
end;

function TStreamOperation<T>.Transform<TOutput>(const AFunction: TMapFunction<T, TOutput>): TArray<TOutput>;
begin
  result := FProcessor.AsStreamProcessor.Map<T, TOutput>(AFunction);
end;

procedure TStreamOperation<T>.Apply(const AFunction: TApplyFunction<T>);
begin
  FProcessor.Apply(AFunction);
end;

{ Stream }

class function Stream.From<T>(const AType: TArray<T>): TStreamOperation<T>;
begin
  result := TStreamOperation<T>.Create(TArrayStreamProcessor<T>.Create(TValue.From < TArray < T >> (AType)));
end;

class function Stream.Filter<T>(const Filter: TFilterFunction<T>): TExpression;
begin
  result := TExpression.Create(TFilterExpr.Create(TTypedFunctionFilterProcessor<T>.Create(Filter)));
end;

class function Stream.From<T>(const AType: TList<T>): TStreamOperation<T>;
begin
  result := TStreamOperation<T>.Create(TListStreamProcessor<T>.Create(TValue.From < TList < T >> (AType)));
end;

class function Stream.From<T>(const AType: IEnumerable<T>): TStreamOperation<T>;
begin
  result := TStreamOperation<T>.Create(TEnumerableStreamProcessor<T>.Create(TValue.From < IEnumerable < T >> (AType)));
end;

end.
