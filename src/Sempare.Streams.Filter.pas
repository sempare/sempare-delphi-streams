unit Sempare.Streams.Filter;

interface

uses
  Sempare.Streams.Types,
  Sempare.Streams.Expr,
  System.Rtti;

type
  TAbstractFilter<T> = class abstract(TInterfacedObject, IFilterFunction, IFilterFunction<T>)
  public
    function IsTrue(const AData: TValue): boolean; overload; virtual; abstract;
    function IsTrue(const AData: T): boolean; overload;
  end;

  TExprFilter<T> = class(TAbstractFilter<T>)
  strict private
    FExpr: TExpr;
  public
    constructor Create(const AExpr: TExpr);
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

implementation

uses
  Sempare.Streams.Rtti;

{ TExprFilter<T> }

constructor TExprFilter<T>.Create(const AExpr: TExpr);
var
  visitor: TRttiExprVisitor;
begin
  visitor := TRttiExprVisitor.Create(RttiCtx.GetType(typeinfo(T)));
  try
    AExpr.Accept(visitor);
  finally
    visitor.Free;
  end;
  FExpr := AExpr;
end;

destructor TExprFilter<T>.Destroy;
begin
  FExpr.Free;
  inherited;
end;

function TExprFilter<T>.IsTrue(const AData: TValue): boolean;
begin
  result := FExpr.IsTrue(AData);
end;

{ TTypedFunctionFilter<T> }

constructor TTypedFunctionFilter<T>.Create(const AFunction: TFilterFunction<T>);
begin
  FFunction := AFunction;
end;

function TTypedFunctionFilter<T>.IsTrue(const AData: TValue): boolean;
begin
  result := FFunction(AData.AsType<T>());
end;

{ TFilterProcessor<T> }

function TAbstractFilter<T>.IsTrue(const AData: T): boolean;
begin
  result := IsTrue(TValue.From<T>(AData));
end;

end.
