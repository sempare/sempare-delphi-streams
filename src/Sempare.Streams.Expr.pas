unit Sempare.Streams.Expr;

interface

uses
  System.Rtti,
  System.SysUtils,
  Sempare.Streams.Types,
  Sempare.Streams.Rtti;

type
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

    // function AsBoolExpr: TBoolExpr;
    // function AsUnaryExpr: TUnaryExpr;
    // function AsBinaryExpr: TBinaryExpr;
    // function AsFieldExpr: TFieldExpr;

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

implementation

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
  if AExpr.Value.TypeInfo = TypeInfo(TFieldExpr) then
    Visit(AExpr.Value.AsType<TFieldExpr>);
end;

{ TFieldExpr }

constructor TFieldExpr.Create(const AField: string);
begin
  FField := AField.Trim();
end;

function TFieldExpr.IsTrue(const [ref] AValue: TValue): boolean;

  function GetValue(out v: TValue): boolean;
  begin
    result := RttiField.GetValue(AValue, v);
  end;

  function GetRightValue(out v: TValue): boolean;
  begin
    result := true;
    v := FValue;
    if FValue.TypeInfo = TypeInfo(TFieldExpr) then
      result := TFieldExpr(FValue.AsObject).RttiField.GetValue(AValue, v);
  end;

  function RaiseOperatorNotSupported: boolean;
  begin
    raise TExprException.Create('operator not supported');
  end;

  function FilterBoolean(const [ref] A, b: TValue): boolean;

    function GetVal(const A: TValue): boolean;
    begin
      result := A.AsBoolean;
    end;

  var
    ab, bb: boolean;

  begin
    ab := GetVal(A);
    bb := GetVal(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
    end;
  end;

  function AsInt64(const [ref] AValue: TValue): int64;
  begin
    case AValue.Kind of
      tkInteger, tkInt64:
        result := AValue.AsInt64;
      tkFloat:
        result := trunc(AValue.AsExtended);
    else
      result := 0;
    end;
  end;

  function FilterInt(const [ref] A, b: TValue): boolean;

    function GetVal(const A: TValue): int64;
    begin
      result := A.AsInt64;
    end;

  var
    ab, bb: int64;

  begin
    ab := GetVal(A);
    bb := AsInt64(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
    end;
  end;

  function AsFloat(const [ref] AValue: TValue): double;
  begin
    case AValue.Kind of
      tkInteger, tkInt64:
        result := AValue.AsInt64;
      tkFloat:
        result := AValue.AsExtended;
    else
      result := 0;
    end;
  end;

  function FilterFloat(const [ref] A, b: TValue): boolean;
    function GetVal(const A: TValue): double;
    begin
      result := A.AsExtended;
    end;

  var
    ab, bb: double;

  begin
    ab := GetVal(A);
    bb := AsFloat(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
    end;
  end;

  function FilterString(const [ref] A, b: TValue): boolean;
    function GetVal(const A: TValue): string;
    begin
      result := A.AsString;
    end;

  var
    ab, bb: string;
  begin
    ab := GetVal(A);
    bb := GetVal(b);
    case FOP of
      foEQ:
        result := ab = bb;
      foNEQ:
        result := ab <> bb;
      foLT:
        result := ab < bb;
      foLTE:
        result := ab <= bb;
      foGT:
        result := ab > bb;
      foGTE:
        result := ab >= bb;
    else
      result := RaiseOperatorNotSupported;
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
      if TypeInfo(boolean) = v.TypeInfo then
        result := FilterBoolean(v, r)
      else
        raise TExprException.Create('enum not supported');
    tkInteger, tkInt64:
      result := FilterInt(v, r);
    tkFloat:
      result := FilterFloat(v, r);
    tkString, tkWideString, tkUnicodeString, tkAnsiString:
      result := FilterString(v, r);
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
  result := etField;
end;

function TFieldExpr.GetField: string;
begin
  result := FField;
end;

function TFieldExpr.GetOP: TFieldExprOper;
begin
  result := FOP;
end;

function TFieldExpr.GetRttiField: IFieldExtractor;
begin
  result := FRttiField;
end;

function TFieldExpr.GetValue: TValue;
begin
  result := FValue;
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
      result := result and FRight.IsTrue(AValue);
    boOR:
      result := result or FRight.IsTrue(AValue);
  end;
end;

function TBinaryExpr.GetExprType: TExprType;
begin
  result := etBinary;
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
      result := not FExpr.IsTrue(AValue);
  else
    raise TExprException.Create('unary type not supported');
  end;
end;

function TUnaryExpr.GetExprType: TExprType;
begin
  result := etUnary;
end;

{ TExpr }

procedure TExpr.Accept(const AVisitor: TExprVisitor);
begin
  if Self is TFieldExpr then
    AVisitor.Visit(Self.AsFieldExpr)
  else if Self is TBinaryExpr then
    AVisitor.Visit(Self.AsBinaryExpr)
  else if Self is TUnaryExpr then
    AVisitor.Visit(Self.AsUnaryExpr)
  else if Self is TBoolExpr then
    AVisitor.Visit(Self.AsBoolExpr)
  else
    raise TExprException.Create('unexpected expression type');
end;

function TExpr.AsBinaryExpr: TBinaryExpr;
begin
  result := Self as TBinaryExpr;
end;

function TExpr.AsBoolExpr: TBoolExpr;
begin
  result := Self as TBoolExpr;
end;

function TExpr.AsFieldExpr: TFieldExpr;
begin
  result := Self as TFieldExpr;
end;

function TExpr.AsUnaryExpr: TUnaryExpr;
begin
  result := Self as TUnaryExpr;
end;

function TExpr.IsExprType(const AExprType: TExprType): boolean;
begin
  result := GetExprType = AExprType;
end;

{ TBoolExpr }

constructor TBoolExpr.Create(const AValue: boolean);
begin
  FValue := AValue;
end;

function TBoolExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  result := FValue;
end;

function TBoolExpr.GetExprType: TExprType;
begin
  result := etBoolean;
end;

{ TFilterExpr }

constructor TFilterExpr.Create(Expr: IFilterFunction);
begin
  FExpr := Expr;
end;

function TFilterExpr.IsTrue(const [ref] AValue: TValue): boolean;
begin
  result := FExpr.IsTrue(AValue);
end;

destructor TFilterExpr.Destroy;
begin
  FExpr := nil;
  inherited;
end;

function TFilterExpr.GetExprType: TExprType;
begin
  result := etFilter;
end;

end.
