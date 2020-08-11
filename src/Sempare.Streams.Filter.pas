unit Sempare.Streams.Filter;

interface

uses
  Sempare.Streams.Types,
  Sempare.Streams.Expr,
  System.Rtti;

type
  
  TFilterProcessor = class abstract(TInterfacedObject, IFilterProcessor)
  public
    function Filter(const AData: TValue): boolean; virtual; abstract;
  end;

  TDynamicFilterProcessor = class(TFilterProcessor)
  strict private
    FExpr: TExpr;
  public
    constructor Create(const AExpr: TExpr; const AType: TRttiType);
    destructor Destroy(); override;
    function Filter(const AData: TValue): boolean; override;
  end;

  TTypedFunctionFilterProcessor<T> = class(TFilterProcessor)
  strict private
    FFunction: TFilterFunction<T>;
  public
    constructor Create(const AFunction: TFilterFunction<T>);
    function Filter(const AData: TValue): boolean; override;
  end;

implementation

{ TDynamicFilterProcessor }

constructor TDynamicFilterProcessor.Create(const AExpr: TExpr; const AType: TRttiType);
var
  visitor: TRttiExprVisitor;
begin
  visitor := TRttiExprVisitor.Create(AType);
  try
    AExpr.Accept(visitor);
  finally
    visitor.Free;
  end;
  FExpr := AExpr;
end;

destructor TDynamicFilterProcessor.Destroy;
begin
  FExpr.Free;
  inherited;
end;

function TDynamicFilterProcessor.Filter(const AData: TValue): boolean;
begin
  result := FExpr.Filter(AData);
end;

{ TTypedFunctionFilterProcessor<T> }

constructor TTypedFunctionFilterProcessor<T>.Create(const AFunction: TFilterFunction<T>);
begin
  FFunction := AFunction;
end;

function TTypedFunctionFilterProcessor<T>.Filter(const AData: TValue): boolean;
begin
  result := FFunction(AData.AsType<T>());
end;

end.
